package common

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

func CloneRepo(repoURL, branchName, pat string) error {
	if pat == "" {
		return errors.New("missing PAT")
	}

	u, err := url.Parse(repoURL)
	if err != nil {
		return err
	}
	if u.Host == "" {
		u, err = url.Parse("https://" + repoURL)
		if err != nil {
			return err
		}
	}
	parts := strings.Split(strings.Trim(strings.TrimSuffix(u.Path, ".git"), "/"), "/")
	if len(parts) < 2 {
		return errors.New("cannot parse owner/repo from URL")
	}
	owner, repo := parts[0], parts[1]

	entries, err := os.ReadDir(".")
	if err != nil {
		return err
	}
	baseDir := "."
	if len(entries) > 0 {
		baseDir = repo
		if err := os.MkdirAll(baseDir, 0o755); err != nil {
			return fmt.Errorf("failed to create target dir %s: %w", baseDir, err)
		}
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: pat})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	r, _, err := client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return errors.New("get repo failed")
	}
	ref := r.GetDefaultBranch()
	if ref == "" {
		ref = "main"
	}
	if branchName != "" {
		ref = branchName
	}

	archiveURL, _, err := client.Repositories.GetArchiveLink(ctx, owner, repo, github.Tarball, &github.RepositoryContentGetOptions{Ref: ref})
	if err != nil {
		return errors.New("get archive link failed")
	}

	resp, err := tc.Get(archiveURL.String())
	if err != nil {
		return errors.New("download failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.New("bad status downloading archive")
	}

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return errors.New("gzip parse failed")
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	var topDir string
	for {
		hdr, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return errors.New("tar read failed")
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
			continue
		}
		name := hdr.Name
		if topDir == "" {
			seg := strings.SplitN(name, "/", 2)
			if len(seg) > 0 {
				topDir = seg[0]
			}
		}
		rel := strings.TrimPrefix(name, topDir+"/")
		if rel == "" {
			continue
		}
		target := filepath.Join(baseDir, rel)
		if hdr.Typeflag == tar.TypeDir {
			if err = os.MkdirAll(target, 0o755); err != nil {
				return errors.New("mkdir failed")
			}
			continue
		}
		if err = os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return errors.New("mkdir parent failed")
		}
		f, e2 := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if e2 != nil {
			return errors.New("file create failed")
		}
		if _, e2 = io.Copy(f, tr); e2 != nil {
			f.Close()
			return errors.New("file write failed")
		}
		f.Close()
	}
	return nil
}
