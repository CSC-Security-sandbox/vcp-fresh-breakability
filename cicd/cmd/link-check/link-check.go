package linkcheck

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/spf13/cobra"
)

const linkCheckConfig = "cicd/cmd/link-check/link-check-config.yaml"

var LinkCheckCmd = &cobra.Command{
	Use:   "link-check",
	Short: "A command to check for broken or outdated links in Markdown files",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := checkLinks()
		if err != nil {
			return err
		}
		return nil
	},
}

var (
	fastMode bool
	dir      string
	file     string
)

func init() {
	LinkCheckCmd.Flags().BoolVar(&fastMode, "fast", false, "Fast mode - skip external URL checking")
	LinkCheckCmd.Flags().StringVar(&dir, "dir", "", "Check specific directory")
	LinkCheckCmd.Flags().StringVar(&file, "file", "", "Check specific file")
}

type LinkCheckConfig struct {
	Run struct {
		SkipDirs  []string `yaml:"skip-dirs"`
		SkipFiles []string `yaml:"skip-files"`
		Timeout   int      `yaml:"timeout"` // Timeout in seconds
		Retries   int      `yaml:"retries"` // Number of retries for failed links
	} `yaml:"run"`
	Links struct {
		SkipPatterns  []string `yaml:"skip-patterns"`  // URL patterns to skip
		AllowInsecure bool     `yaml:"allow-insecure"` // Allow HTTP links
		FastMode      bool     `yaml:"fast-mode"`      // Skip external URL checking
	} `yaml:"links"`
}

type LinkResult struct {
	File   string
	Line   int
	URL    string
	Status int
	Error  string
	Valid  bool
}

type LinkChecker struct {
	config    LinkCheckConfig
	client    *http.Client
	skipDirs  map[string]struct{}
	skipFiles map[string]struct{}
	skipRegex []*regexp.Regexp
	results   []LinkResult
}

func readLinkCheckConfig(configPath string) (LinkCheckConfig, error) {
	var config LinkCheckConfig
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Return default config if file doesn't exist
		config.Run.Timeout = 10
		config.Run.Retries = 2
		config.Links.AllowInsecure = false
		return config, nil
	}
	err = yaml.Unmarshal(data, &config)
	return config, err
}

func NewLinkChecker(configPath string) (*LinkChecker, error) {
	config, err := readLinkCheckConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	// Preload skip directories and files into maps for faster lookup
	skipDirs := make(map[string]struct{})
	for _, dir := range config.Run.SkipDirs {
		skipDirs[dir] = struct{}{}
	}

	skipFiles := make(map[string]struct{})
	for _, file := range config.Run.SkipFiles {
		skipFiles[file] = struct{}{}
	}

	// Compile skip patterns
	var skipRegex []*regexp.Regexp
	for _, pattern := range config.Links.SkipPatterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			log.Printf("Warning: Invalid skip pattern '%s': %v", pattern, err)
			continue
		}
		skipRegex = append(skipRegex, regex)
	}

	// Create HTTP client with timeout
	timeout := time.Duration(config.Run.Timeout) * time.Second
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Allow up to 10 redirects
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	return &LinkChecker{
		config:    config,
		client:    client,
		skipDirs:  skipDirs,
		skipFiles: skipFiles,
		skipRegex: skipRegex,
		results:   make([]LinkResult, 0),
	}, nil
}

func (lc *LinkChecker) shouldSkipURL(urlStr string) bool {
	for _, regex := range lc.skipRegex {
		if regex.MatchString(urlStr) {
			return true
		}
	}
	return false
}

func (lc *LinkChecker) isLocalFile(path string) bool {
	// Check if it's a relative path or file:// URL
	return !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://")
}

func (lc *LinkChecker) checkLocalFile(filePath string, baseDir string) LinkResult {
	// Handle relative paths
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(baseDir, filePath)
	}

	// Clean the path
	filePath = filepath.Clean(filePath)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return LinkResult{
			File:   baseDir,
			URL:    filePath,
			Status: 404,
			Error:  "File not found",
			Valid:  false,
		}
	}

	return LinkResult{
		File:   baseDir,
		URL:    filePath,
		Status: 200,
		Valid:  true,
	}
}

func (lc *LinkChecker) checkHTTPLink(urlStr string) LinkResult {
	// Check if we should skip this URL
	if lc.shouldSkipURL(urlStr) {
		return LinkResult{
			URL:   urlStr,
			Valid: true,
		}
	}

	// Fast mode: skip all external URLs
	if lc.config.Links.FastMode || fastMode {
		return LinkResult{
			URL:   urlStr,
			Valid: true, // Assume valid in fast mode
		}
	}

	// Check if insecure HTTP is allowed
	if strings.HasPrefix(urlStr, "http://") && !lc.config.Links.AllowInsecure {
		return LinkResult{
			URL:   urlStr,
			Error: "HTTP links are not allowed (use HTTPS)",
			Valid: false,
		}
	}

	// Retry logic
	var lastErr error
	for attempt := 0; attempt <= lc.config.Run.Retries; attempt++ {
		resp, err := lc.client.Get(urlStr)
		if err != nil {
			lastErr = err
			if attempt < lc.config.Run.Retries {
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
			return LinkResult{
				URL:   urlStr,
				Error: err.Error(),
				Valid: false,
			}
		}
		// ensure body is closed and ignore close error explicitly
		defer func() { _ = resp.Body.Close() }()

		// Check status code
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			return LinkResult{
				URL:    urlStr,
				Status: resp.StatusCode,
				Valid:  true,
			}
		}

		return LinkResult{
			URL:    urlStr,
			Status: resp.StatusCode,
			Error:  fmt.Sprintf("HTTP %d", resp.StatusCode),
			Valid:  false,
		}
	}

	if lastErr != nil {
		return LinkResult{
			URL:   urlStr,
			Error: lastErr.Error(),
			Valid: false,
		}
	}

	return LinkResult{
		URL:   urlStr,
		Error: "unknown error",
		Valid: false,
	}
}

func (lc *LinkChecker) extractLinksFromMarkdown(filePath string) ([]LinkResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var results []LinkResult
	scanner := bufio.NewScanner(file)
	lineNum := 0
	baseDir := filepath.Dir(filePath)

	// Regex patterns for different link types in Markdown
	linkPatterns := []*regexp.Regexp{
		// [text](url) format
		regexp.MustCompile(`\[(.*?)\](?:\((.*?)\))`),
		// [text][ref] format (reference links)
		regexp.MustCompile(`\[(.*?)\](?:\[(.*?)\])`),
		// <url> format
		regexp.MustCompile(`<([^>]+)>`),
		// ![alt](url) format (images)
		regexp.MustCompile(`!\[(.*?)\](?:\((.*?)\))`),
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		for _, pattern := range linkPatterns {
			matches := pattern.FindAllStringSubmatch(line, -1)
			for _, match := range matches {
				var urlStr string
				if len(match) >= 3 {
					urlStr = match[2] // Second capture group is usually the URL
				} else if len(match) >= 2 {
					urlStr = match[1] // First capture group for <url> format
				}

				if urlStr == "" {
					continue
				}

				// Skip anchor-only links
				if strings.HasPrefix(urlStr, "#") {
					continue
				}

				var result LinkResult
				if lc.isLocalFile(urlStr) {
					result = lc.checkLocalFile(urlStr, baseDir)
				} else {
					result = lc.checkHTTPLink(urlStr)
				}

				result.File = filePath
				result.Line = lineNum
				results = append(results, result)
			}
		}
	}

	return results, scanner.Err()
}

func (lc *LinkChecker) checkFile(filePath string) error {
	// Check if file should be skipped
	if _, found := lc.skipFiles[filePath]; found {
		return nil
	}

	// Check if file is in a skipped directory
	for skipDir := range lc.skipDirs {
		if strings.HasPrefix(filePath, skipDir) {
			return nil
		}
	}

	// Only process Markdown files
	if !strings.HasSuffix(filePath, ".md") {
		return nil
	}

	results, err := lc.extractLinksFromMarkdown(filePath)
	if err != nil {
		return fmt.Errorf("failed to extract links from %s: %w", filePath, err)
	}

	lc.results = append(lc.results, results...)
	return nil
}

func (lc *LinkChecker) checkDirectory(rootPath string) error {
	return filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			if _, found := lc.skipDirs[path]; found {
				return filepath.SkipDir
			}
			return nil
		}

		return lc.checkFile(path)
	})
}

func (lc *LinkChecker) printResults() {
	validCount := 0
	invalidCount := 0

	log.Println("Link Check Results:")
	log.Println("==================")

	for _, result := range lc.results {
		if result.Valid {
			validCount++
			if result.Status > 0 {
				log.Printf("✓ %s:%d - %s (HTTP %d)", result.File, result.Line, result.URL, result.Status)
			} else {
				log.Printf("✓ %s:%d - %s", result.File, result.Line, result.URL)
			}
		} else {
			invalidCount++
			if result.Status > 0 {
				log.Printf("✗ %s:%d - %s (HTTP %d: %s)", result.File, result.Line, result.URL, result.Status, result.Error)
			} else {
				log.Printf("✗ %s:%d - %s (%s)", result.File, result.Line, result.URL, result.Error)
			}
		}
	}

	log.Printf("\nSummary: %d valid links, %d invalid links", validCount, invalidCount)
}

func checkLinks() error {
	checker, err := NewLinkChecker(linkCheckConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize link checker: %w", err)
	}

	// Check specific file if provided
	if file != "" {
		err = checker.checkFile(file)
		if err != nil {
			return fmt.Errorf("failed to check file: %w", err)
		}
	} else if dir != "" {
		// Check specific directory if provided
		err = checker.checkDirectory(dir)
		if err != nil {
			return fmt.Errorf("failed to check directory: %w", err)
		}
	} else {
		// Check all Markdown files in the repository
		err = checker.checkDirectory(".")
		if err != nil {
			return fmt.Errorf("failed to check directory: %w", err)
		}
	}

	checker.printResults()

	// Check if there are any invalid links
	hasInvalidLinks := false
	for _, result := range checker.results {
		if !result.Valid {
			hasInvalidLinks = true
			break
		}
	}

	if hasInvalidLinks {
		return fmt.Errorf("found invalid links")
	}

	return nil
}
