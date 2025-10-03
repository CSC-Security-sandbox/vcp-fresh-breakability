// filepath: scripts/link-checker/link-checker.go
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	// Support flags used by CI/workflow
	var dir string
	var verbose bool
	flag.StringVar(&dir, "dir", "doc/", "directory to check for markdown links")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose output")
	flag.Parse()

	// Allow positional arg to remain compatible with older usage
	if flag.NArg() > 0 {
		// if a positional arg is provided, prefer it over the default
		dir = flag.Arg(0)
	}

	if verbose {
		log.Printf("Checking links in %s (verbose)", dir)
	} else {
		log.Printf("Checking links in %s (fast mode - local files only)", dir)
	}

	var brokenLinks []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(path, ".md") {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()

		scanner := bufio.NewScanner(file)
		lineNum := 0
		baseDir := filepath.Dir(path)

		// Regex for markdown links - avoid redundant escape in character class
		linkRegex := regexp.MustCompile(`\[([^]]+)\]\(([^)]+)\)`)

		for scanner.Scan() {
			lineNum++
			line := scanner.Text()
			matches := linkRegex.FindAllStringSubmatch(line, -1)

			for _, match := range matches {
				if len(match) >= 3 {
					url := match[2]

					// Skip external URLs in fast mode
					if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
						if verbose {
							log.Printf("Skipping external URL: %s in %s:%d", url, path, lineNum)
						}
						continue
					}

					// Skip anchor links
					if strings.HasPrefix(url, "#") {
						continue
					}

					// Check local file
					if !filepath.IsAbs(url) {
						url = filepath.Join(baseDir, url)
					}
					url = filepath.Clean(url)

					if _, err := os.Stat(url); os.IsNotExist(err) {
						brokenLinks = append(brokenLinks, fmt.Sprintf("%s:%d - %s", path, lineNum, match[2]))
						if verbose {
							log.Printf("Broken: %s -> %s", path, url)
						}
					}
				}
			}
		}

		return scanner.Err()
	})

	if err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}

	if len(brokenLinks) > 0 {
		log.Println("Broken links found:")
		for _, link := range brokenLinks {
			log.Printf("✗ %s", link)
		}
		os.Exit(1)
	} else {
		log.Println("✓ All local links are valid!")
	}
}
