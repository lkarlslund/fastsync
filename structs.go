package main

import "time"

type Command struct {
	Cmd string
}

type Result struct {
	Type    string
	Results interface{}
}

type Filelist []FileInfo

type FileInfo struct {
	Inode                              uint64
	Filename                           string
	ModifyTime, AccessTime, CreateTime time.Time
	Owner, Group                       uint32
	Permissions                        uint16
}
