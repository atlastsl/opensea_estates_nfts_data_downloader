package helpers

import "path/filepath"

func ListFiles(parentDir, pattern string, fullPath bool) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(parentDir, pattern))
	if err == nil && len(files) > 0 && !fullPath {
		files = ArrayMap(files, func(t string) (bool, string) {
			return true, filepath.Base(t)
		}, true, "")
	}
	return files, err
}
