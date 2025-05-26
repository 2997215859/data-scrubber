package service

import (
	"github.com/xitongsys/parquet-go/source"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetWriter 封装Parquet写入功能
type ParquetWriter struct {
	fileWriter    source.ParquetFile
	parquetWriter *writer.ParquetWriter
}

// NewParquetWriter 创建一个新的Parquet写入器
func NewParquetWriter(filePath string, schema interface{}) (*ParquetWriter, error) {
	// 创建本地文件写入器
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return nil, err
	}

	// 创建Parquet写入器
	pw, err := writer.NewParquetWriter(fw, schema, 4)
	if err != nil {
		fw.Close() // 这里使用Close()是正确的，因为它是io.Closer接口方法
		return nil, err
	}

	// 设置Parquet配置
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	return &ParquetWriter{
		fileWriter:    fw,
		parquetWriter: pw,
	}, nil
}

// Write 写入单行数据
func (pw *ParquetWriter) Write(row interface{}) error {
	return pw.parquetWriter.Write(row)
}

// WriteBatch 批量写入多行数据
func (pw *ParquetWriter) WriteBatch(rows []interface{}) error {
	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭写入器并释放资源
func (pw *ParquetWriter) Close() error {
	// 先停止Parquet写入器
	if err := pw.parquetWriter.WriteStop(); err != nil {
		_ = pw.fileWriter.Close() // 忽略错误，因为我们要返回主要错误
		return err
	}

	// 然后关闭文件
	return pw.fileWriter.Close()
}

// 示例使用
func ExampleUsage() {
	// 定义数据结构
	type Student struct {
		Name   string  `parquet:"name=name, type=UTF8"`
		Age    int32   `parquet:"name=age, type=INT32"`
		Weight float32 `parquet:"name=weight, type=FLOAT"`
	}

	// 创建写入器
	pw, err := NewParquetWriter("output.parquet", new(Student))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := pw.Close(); err != nil {
			log.Printf("关闭Parquet写入器时出错: %v", err)
		}
	}()

	// 写入单行
	student1 := Student{
		Name:   "张三",
		Age:    20,
		Weight: 65.5,
	}
	if err := pw.Write(student1); err != nil {
		log.Fatal(err)
	}

	// 批量写入
	students := []interface{}{
		Student{Name: "李四", Age: 21, Weight: 70.1},
		Student{Name: "王五", Age: 22, Weight: 68.3},
	}
	if err := pw.WriteBatch(students); err != nil {
		log.Fatal(err)
	}

	log.Println("Parquet文件写入完成")
}
