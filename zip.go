package adapters

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type ErrInvalidPath struct {
	path string
}

func (e *ErrInvalidPath) Error() string {
	return "invalid file path: " + e.path
}

type ZipAdapter struct{}

func NewZipAdapter() *ZipAdapter {
	return &ZipAdapter{}
}

func (a *ZipAdapter) Zip(ctx context.Context, source string, filename string, dst io.Writer) error {
	writer := zip.NewWriter(dst)
	defer writer.Close()

	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		header.Method = zip.Deflate

		if header.Name, err = filepath.Rel(filepath.Dir(source), path); err != nil {
			return err
		}

		if info.IsDir() {
			header.Name += "/"
		}

		headerWriter, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(headerWriter, f); err != nil {
			return err
		}

		return nil
	})
}

func (a *ZipAdapter) Unzip(ctx context.Context, source string, destination string) error {
	reader, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}

	for _, f := range reader.File {
		if err := a.unzipFile(f, destination); err != nil {
			return err
		}
	}

	return nil
}

func (a *ZipAdapter) unzipFile(f *zip.File, destination string) error {
	filePath := filepath.Join(destination, f.Name)

	if !strings.HasPrefix(filePath, filepath.Clean(destination)+string(os.PathSeparator)) {
		return &ErrInvalidPath{filePath}
	}

	if f.FileInfo().IsDir() {
		if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
			return err
		}

		return nil
	}

	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	destinationFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	zippedFile, err := f.Open()
	if err != nil {
		return err
	}
	defer zippedFile.Close()

	if _, err := io.Copy(destinationFile, zippedFile); err != nil {
		return err
	}

	return nil
}
