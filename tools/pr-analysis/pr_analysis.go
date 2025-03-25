package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

// Write analysis result to a markdown file
func writeToMarkdown(filePath string, content string) error {
	return ioutil.WriteFile(filePath, []byte(content), 0644)
}

// Read the prompt from a file
func readPromptFromFile(filePath string) (string, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read prompt file: %v", err)
	}
	return string(content), nil
}

func readPrompt() string {
	return `Please conduct a **focused and concise code review** of the provided code changes (diff). Only analyze the changes themselves, using the surrounding lines for context where necessary. Avoid speculative feedback or false positives.

**Instructions:**

1. **Change Analysis:**
   - **Before:** In 1-2 sentences, describe the state of the code before the changes
   - **After:** In 1-2 sentences, summarize the modifications made.
   - **Rationale:** In 1-2 sentences, explain the purpose of the changes (e.g., bug fix, feature addition, optimization). If unclear, note this.

2. **Code Review:**
   - **Quality:** Briefly assess the quality of the changes (e.g., "well-implemented," "introduces potential issues").
   - **Readability:** Comment on the clarity and maintainability of the changes (e.g., "clear and concise," "naming could be improved").
   - **Potential Issues:** Highlight any obvious bugs, edge cases, or side effects.
   - **Best Practices:** Note adherence to or deviations from coding standards and best practices.
   - **Suggestions:** Provide 1-2 actionable suggestions for improvement, if applicable.

3. **Comments:**
   - Add **specific, concise comments** directly on the diff (if possible) to highlight key areas of concern or commendation.
   - Avoid generalities—focus on actionable insights.

**Deliverables:**
- A **short, focused analysis** (3-5 sentences total) summarizing the changes and their impact.

**Rules:**
- **Strictly focus on the diff.** Do not analyze surrounding code unless directly relevant.
- Avoid speculative feedback. Only raise concerns if directly supported by the changes.
- Keep the analysis **crisp and concise**. Avoid unnecessary elaboration.`
}

// Call OpenAI API to analyze diff and return the response content
func analyzeDiff(apiKey string, diffContent string, userName string, prompt string) (string, error) {
	url := "https://llm-proxy-api.ai.openeng.netapp.com/v1/chat/completions"

	// Construct request payload
	requestBody := map[string]interface{}{
		"model": "o1-mini",
		"messages": []map[string]string{
			{
				"role":    "user",
				"content": fmt.Sprintf(prompt, diffContent),
			},
		},
		"user": userName,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %v", err)
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonBody)))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))

	// Modify http.Client to skip TLS verification in GitHub Actions
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	// Parse OpenAI response
	var openAIResponse map[string]interface{}
	if err := json.Unmarshal(body, &openAIResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Extract the content from the response
	choices := openAIResponse["choices"].([]interface{})
	firstChoice := choices[0].(map[string]interface{})
	message := firstChoice["message"].(map[string]interface{})
	return message["content"].(string), nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <diff_path> <output_path>", os.Args[0])
	}

	diffPath := os.Args[1]
	outputPath := os.Args[2]
	//promptPath := "cmd/pr-analysis/instructions.txt"
	userName := os.Getenv("OPENAI_USER")
	if userName == "" {
		log.Fatalf("USER_NAME environment variable is not set")
	}

	// Load the diff content from the file
	diffContent, err := ioutil.ReadFile(diffPath)
	if err != nil {
		log.Fatalf("Failed to read diff file: %v", err)
	}

	apiKey := os.Getenv("OPENAI_KEY")
	if apiKey == "" {
		log.Fatalf("OPENAI_KEY environment variable is not set")
	}

	//// Load the prompt from the file
	//prompt, err := readPromptFromFile(promptPath)
	//if err != nil {
	// log.Fatalf("Failed to read prompt file: %v", err)
	//}

	prompt := readPrompt()

	// Analyze the diff using OpenAI API
	analysis, err := analyzeDiff(apiKey, string(diffContent), userName, prompt)
	if err != nil {
		log.Fatalf("Failed to analyze diff: %v", err)
	}

	// Write the analysis result to the markdown file
	if err := writeToMarkdown(outputPath, analysis); err != nil {
		log.Fatalf("Failed to write analysis to markdown: %v", err)
	}
	fmt.Printf("Analysis written to: %s\n", outputPath)
}
