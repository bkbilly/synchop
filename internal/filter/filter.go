package filter

import (
	"os"
	"path/filepath"
	"strings"
)

type FilterMode string

const (
	ModeAll         FilterMode = "all"
	ModeIncludeOnly FilterMode = "include_only"
	ModeExclude     FilterMode = "exclude"
)

func ParsePatterns(text string) []string {
	var pats []string
	raw := strings.FieldsFunc(text, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	for _, p := range raw {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			pats = append(pats, trimmed)
		}
	}
	return pats
}

type Filter struct {
	Mode     FilterMode
	Patterns []string
}

func NewFilter(modeStr, patternsText string) *Filter {
	mode := FilterMode(modeStr)
	if mode != ModeIncludeOnly && mode != ModeExclude {
		mode = ModeAll
	}
	return &Filter{
		Mode:     mode,
		Patterns: ParsePatterns(patternsText),
	}
}

// ShouldKeep returns true if the file or directory should be synced
func (f *Filter) ShouldKeep(relPath string, isDir bool) bool {
	if f.Mode == ModeAll || len(f.Patterns) == 0 {
		return true
	}

	cleanPath := filepath.ToSlash(filepath.Clean(relPath))
	cleanPath = strings.TrimPrefix(cleanPath, "./")
	cleanPath = strings.TrimPrefix(cleanPath, "/")
	baseName := filepath.Base(cleanPath)

	if isDir {
		if f.Mode == ModeIncludeOnly {
			// In include-only mode, allow directory traversal to find matching files
			return true
		}
		// In exclude mode, check if directory itself matches an exclude pattern
		for _, pat := range f.Patterns {
			cleanPat := strings.TrimSuffix(pat, "/")
			if matched, _ := filepath.Match(cleanPat, baseName); matched {
				return false
			}
			if matched, _ := filepath.Match(cleanPat, cleanPath); matched {
				return false
			}
		}
		return true
	}

	// For files:
	if f.Mode == ModeIncludeOnly {
		for _, pat := range f.Patterns {
			if matched, _ := filepath.Match(pat, baseName); matched {
				return true
			}
			if matched, _ := filepath.Match(pat, cleanPath); matched {
				return true
			}
		}
		return false
	}

	if f.Mode == ModeExclude {
		for _, pat := range f.Patterns {
			cleanPat := strings.TrimSuffix(pat, "/")
			if matched, _ := filepath.Match(cleanPat, baseName); matched {
				return false
			}
			if matched, _ := filepath.Match(cleanPat, cleanPath); matched {
				return false
			}
			// Check parent directories
			dirPath := filepath.Dir(cleanPath)
			if dirPath != "." && dirPath != "" {
				segments := strings.Split(dirPath, "/")
				for _, seg := range segments {
					if m, _ := filepath.Match(cleanPat, seg); m {
						return false
					}
				}
			}
		}
		return true
	}

	return true
}

// ToRsyncArgs converts filter configuration into rsync command arguments
func ToRsyncArgs(modeStr, patternsText string) []string {
	mode := FilterMode(modeStr)
	patterns := ParsePatterns(patternsText)
	if len(patterns) == 0 || mode == ModeAll {
		return nil
	}

	var args []string
	if mode == ModeIncludeOnly {
		args = append(args, "--include=*/")
		for _, pat := range patterns {
			args = append(args, "--include="+pat)
		}
		args = append(args, "--exclude=*")
		args = append(args, "--prune-empty-dirs")
		args = append(args, "--delete-excluded")
	} else if mode == ModeExclude {
		for _, pat := range patterns {
			args = append(args, "--exclude="+pat)
		}
		args = append(args, "--delete-excluded")
	}
	return args
}

// WalkDirWithSymlinks recursively walks root, resolving top-level or directory symlinks
func WalkDirWithSymlinks(root string, walkFn func(relPath string, info os.FileInfo, fullPath string) error) error {
	var walkSub func(currentDir, currentRel string) error
	walkSub = func(currentDir, currentRel string) error {
		entries, err := os.ReadDir(currentDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			subPath := filepath.Join(currentDir, name)
			subRel := filepath.Join(currentRel, name)

			fi, err := entry.Info()
			if err != nil {
				continue
			}

			// If symlink, resolve target info
			if fi.Mode()&os.ModeSymlink != 0 {
				if realFi, err := os.Stat(subPath); err == nil {
					fi = realFi
				}
			}

			if err := walkFn(subRel, fi, subPath); err != nil {
				if err == filepath.SkipDir {
					continue
				}
				return err
			}

			if fi.IsDir() {
				if err := walkSub(subPath, subRel); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walkSub(root, "")
}
