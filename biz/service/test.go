package service

import (
	"bytes"
	"io"
)

//type YourStruct struct {
//	// 定义你的结构体字段
//}

// 自定义Reader实现流式去除行尾逗号
type streamingTrimCommaReader struct {
	src      io.Reader
	buffer   []byte
	leftover []byte
}

func (r *streamingTrimCommaReader) Read(p []byte) (n int, err error) {
	// 添加日志查看实际读取的数据
	//logger.Info("Reading data...")

	// 处理上次剩余的数据
	if len(r.leftover) > 0 {
		n = copy(p, r.leftover)
		r.leftover = r.leftover[n:]
		return n, nil
	}

	// 从源读取数据
	n, err = r.src.Read(p)
	if n == 0 {
		return 0, err
	}

	// 处理缓冲区
	data := p[:n]
	lines := bytes.SplitAfter(data, []byte{'\n'})

	var cleaned []byte
	for i, line := range lines {
		// 最后一行可能不完整，保留到下次处理
		if i == len(lines)-1 && err == nil && !bytes.HasSuffix(line, []byte{'\n'}) {
			r.leftover = append(r.leftover, line...)
			continue
		}

		// 去除行尾逗号
		trimmed := bytes.TrimSuffix(line, []byte{','})
		if bytes.HasSuffix(line, []byte{',', '\n'}) {
			trimmed = append(bytes.TrimSuffix(line, []byte{',', '\n'}), '\n')
		}
		cleaned = append(cleaned, trimmed...)
	}

	// 将处理后的数据复制回p
	n = copy(p, cleaned)
	if n < len(cleaned) {
		r.leftover = cleaned[n:]
	}

	return n, err
}

//
//func streamZippedCSV(ctx context.Context, zipPath string, outputChan chan<- YourStruct) error {
//	// 打开ZIP文件
//	r, err := zip.OpenReader(zipPath)
//	if err != nil {
//		return err
//	}
//	defer r.Close()
//
//	// 假设ZIP中只有一个CSV文件
//	if len(r.File) == 0 {
//		return io.EOF
//	}
//
//	csvFile, err := r.File[0].Open()
//	if err != nil {
//		return err
//	}
//	defer csvFile.Close()
//
//	// 创建自定义Reader处理多余逗号
//	trimmedReader := &streamingTrimCommaReader{src: csvFile}
//
//	// 使用UnmarshalToChan流式处理
//	if err := gocsv.UnmarshalToChan(trimmedReader, outputChan); err != nil {
//		return err
//	}
//
//	return nil
//}
