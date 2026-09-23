package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/spf13/cobra"
	"github.com/ugorji/go/codec"
)

func newVerifyCommand() *cobra.Command {
	var report, sourcePath string
	var includes []string
	var attrs, hardlinks bool
	command := &cobra.Command{
		Use: "verify server:port", Short: "Read-only archive verification with a JSON-lines report", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			password, e := readPasswordFile(passwordFile)
			if e != nil {
				return e
			}
			var output io.Writer = cmd.OutOrStdout()
			if report != "" {
				// Reports must not change the tree being verified, or overwrite an older report.
				root, e := fastsync.DirectoryPathNoFollow(directory)
				if e != nil {
					return e
				}
				root, e = filepath.Abs(root)
				if e != nil {
					return e
				}
				parent, e := fastsync.DirectoryPathNoFollow(filepath.Dir(report))
				if e != nil {
					return e
				}
				absolute, e := filepath.Abs(filepath.Join(parent, filepath.Base(report)))
				if e != nil {
					return e
				}
				rel, e := filepath.Rel(root, absolute)
				if e != nil {
					return e
				}
				if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					return fmt.Errorf("report must be outside the archive directory")
				}
				file, e := fastsync.OpenFileNoFollow(absolute, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if e != nil {
					return e
				}
				defer func() { err = errors.Join(err, file.Sync(), file.Close()) }()
				output = file
			}
			conn, err := net.DialTimeout("tcp", args[0], 30*time.Second)
			if err != nil {
				return err
			}
			var h codec.MsgpackHandle
			rpcClient := rpc.NewClientWithCodec(codec.GoRpc.ClientCodec(fastsync.CompressedReadWriteCloser(conn), &h))
			defer func() { err = errors.Join(err, rpcClient.Close()) }()
			client := fastsync.NewClient()
			client.Password = password
			client.BasePath = directory
			client.SourcePath = sourcePath
			client.Include = includes
			client.Options.SendXattr = attrs
			client.PreserveHardlinks = hardlinks
			return client.Verify(rpcClient, output)
		},
	}
	command.Flags().StringVar(&sourcePath, "source", "", "Subdirectory of the remote server root")
	command.Flags().StringArrayVar(&includes, "include", nil, "Top-level source name or glob to verify recursively (repeatable)")
	command.Flags().StringVar(&report, "report", "", "Save JSON-lines report outside archive (default stdout)")
	command.Flags().BoolVar(&attrs, "xattr", true, "Verify extended attributes")
	command.Flags().BoolVar(&hardlinks, "hardlinks", true, "Verify hardlink relationships")
	return command
}
