package main

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/colinmarc/hdfs"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

// HdfsClient ...
type HdfsClient struct {
	client *hdfs.Client
}

func main() {
	app := cli.NewApp()
	app.Name = "hdfscp"
	app.Usage = "HDFS remote upload/download directory or file"
	app.Version = "0.0.1"

	app.Commands = []cli.Command{
		{
			Name:    "upload",
			Aliases: []string{"u"},
			Usage:   "upload local dir orfile to hdfs",
			Action: func(c *cli.Context) error {
				var client = NewClient()
				var args = c.Args()
				log.Infof("upload args: %v", args)

				return client.Upload(args.Get(0), args.Get(1))
			},
		},
		{
			Name:    "download",
			Aliases: []string{"d"},
			Usage:   "download remote hdfs dir or file to local",
			Action: func(c *cli.Context) error {
				var client = NewClient()
				var args = c.Args()
				log.Infof("download args: %v", args)

				return client.Download(args.Get(0), args.Get(1))
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func New(client *hdfs.Client) *HdfsClient {
	return &HdfsClient{client}
}

func NewClient() *HdfsClient {
	address := os.Getenv("HADOOP_ADDRESS")
	username := os.Getenv("HADOOP_USER_NAME")
	client, err := hdfs.NewForUser(address, username)
	if err != nil {
		panic(err)
	}

	return &HdfsClient{client}
}

type statFunc func(*hdfs.Client, string) (os.FileInfo, error)
type readDirFunc func(*hdfs.Client, string) ([]os.FileInfo, error)
type copyFileFunc func(*hdfs.Client, string, string) error
type mkdirFunc func(*hdfs.Client, string, bool) error

func (c *HdfsClient) Download(src string, dst string) error {
	return c.Scp(src, dst, hdfsStat, readHdfsDir, copy2Local, osMkdir)
}

func (c *HdfsClient) Upload(src string, dst string) error {
	return c.Scp(src, dst, osStat, readLocalDir, copy2Remote, hdfsMkdir)
}

func (c *HdfsClient) Scp(src string, dst string, stat statFunc, readDir readDirFunc, copyFile copyFileFunc, mkdir mkdirFunc) error {
	fileInfo, err := stat(c.client, src)
	if err != nil {
		return err
	}

	mkdir(c.client, dst, true)
	if fileInfo.IsDir() {
		return c.WalkDir(src, dst, "", readDir, copyFile, mkdir)
	} else {
		return copyFile(c.client, src, dst)
	}
}

func (c *HdfsClient) WalkDir(src string, dst string, dir string, readDir readDirFunc, copyFile copyFileFunc, mkdir mkdirFunc) error {
	srcDirPath := path.Join(src, dir)
	dstDirPath := path.Join(dst, dir)

	fileInfo, err := readDir(c.client, srcDirPath)

	if err != nil {
		log.Errorf("Failed to read dir: %s, error: %v", srcDirPath, err)
		return err
	}

	for _, f := range fileInfo {
		srcPath := path.Join(srcDirPath, f.Name())
		dstPath := path.Join(dstDirPath, f.Name())

		if f.IsDir() {
			mkdir(c.client, dstPath, false)
			c.WalkDir(srcDirPath, dstDirPath, f.Name(), readDir, copyFile, mkdir)
		} else {
			if err := copyFile(c.client, srcPath, dstPath); err != nil {
				log.Errorf("Failed to copy file from %s to %s, %v", srcPath, dstPath, err)
				return err
			}
		}
	}

	return nil
}

func hdfsStat(client *hdfs.Client, dir string) (os.FileInfo, error) {
	return client.Stat(dir)
}

func osStat(client *hdfs.Client, dir string) (os.FileInfo, error) {
	return os.Stat(dir)
}

func readHdfsDir(client *hdfs.Client, dir string) ([]os.FileInfo, error) {
	return client.ReadDir(dir)
}

func readLocalDir(client *hdfs.Client, dir string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dir)
}

func copy2Local(client *hdfs.Client, src string, dst string) error {
	// TODO: add IsSimilarFile
	if err := client.CopyToLocal(src, dst); err != nil {
		return client.CopyToLocal(src, dst)
	}

	return nil
}

func copy2Remote(client *hdfs.Client, src string, dst string) error {
	if err := client.CopyToRemote(src, dst); err != nil {
		return client.CopyToRemote(src, dst)
	}

	return nil
}

func osMkdir(client *hdfs.Client, dir string, isAll bool) error {
	if isAll {
		os.MkdirAll(dir, os.ModePerm)
	}
	return os.Mkdir(dir, os.ModePerm)
}

func hdfsMkdir(client *hdfs.Client, dir string, isAll bool) error {
	if isAll {
		client.MkdirAll(dir, os.ModePerm)
	}
	return client.Mkdir(dir, os.ModePerm)
}
