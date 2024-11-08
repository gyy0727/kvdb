package wal

import (
	"fmt"
	"path/filepath"
)

//*根据给定的目录路径（dirPath）、文件扩展名（extName）和段ID（id）来构建一个文件名
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}
