/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"sync" //Sync包同步提供基本的同步原语，如互斥锁
	"gopkg.in/cheggaaa/pb.v1" //简单的控制台进度条
	log "github.com/cihub/seelog" // 本地日志库
	"os"
	"bufio" //bufio模块通过对io模块的封装，提供了数据缓冲功能，能够一定程度减少大块数据读写带来的开销。
	"encoding/json"
	"io"
)

func checkFileIsExist(filename string) (bool) {
	var exist = true;
	if _, err := os.Stat(filename); os.IsNotExist(err) { // Stat 返回描述文件的 FileInfo 结构。如果有错误，它将是 * PathError 类型。
		exist = false;
	}
	return exist;
}

/**
* 文件读取（从文件中读数据）
*/
func (m *Migrator) NewFileReadWorker(pb *pb.ProgressBar, wg *sync.WaitGroup)  {
	log.Debug("start reading file")
	f, err := os.Open(m.Config.DumpInputFile) // 从文件中都数据
	if err != nil {
		log.Error(err)
		return
	}

	defer f.Close()　// defer 延迟函数，函数退出时执行，关闭文件打开的文件句柄
	r := bufio.NewReader(f)　// NewReader 返回一个新的 Reader ，其缓冲区具有默认大小
	lineCount := 0
	for{
		// ReadString 进行读取，直到输入中第一次出现 \n，返回一个包含数据的字符串直到并包含分隔符。
		//如果 ReadString 在查找分隔符之前遇到错误，它将返回在错误之前读取的数据和错误本身（通常为 io.EOF）。当且仅当返回的数据没有以分隔符结束时，ReadString 返回 err != nil。
		line,err := r.ReadString('\n') 
		if io.EOF == err || nil != err{
			break
		}
		lineCount += 1
		js := map[string]interface{}{}

		//log.Trace("reading file,",lineCount,",", line)
		err = json.Unmarshal([]byte(line), &js) // 把 JSON 转换回对象,解析后的数据存储在第二个参数中
		if(err!=nil){
			log.Error(err)
			continue
		}
		m.DocChan <- js　// 写入通道中
		pb.Increment()　// 进度条增加
	}

	defer f.Close()
	log.Debug("end reading file")
	close(m.DocChan) // 关闭通道
	wg.Done()　//　锁组-1
}

/**
*　文件备份(导出来的是个文件)
*/
func (c *Migrator) NewFileDumpWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	var f *os.File
	var err1   error;

	if checkFileIsExist(c.Config.DumpOutFile) {
		f, err1 = os.OpenFile(c.Config.DumpOutFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)　// 文件存在追加
		if(err1!=nil){
			log.Error(err1)
			return
		}

	}else {
		f, err1 = os.Create(c.Config.DumpOutFile)　// 文件不存在，则新建
		if(err1!=nil){
			log.Error(err1)
			return
		}
	}

	w := bufio.NewWriter(f)　// 返回一个新的 Writer，其缓冲区具有默认大小

	READ_DOCS:
	for {
		docI, open := <-c.DocChan　// 读取通道里的数据

		// this check is in case the document is an error with scroll stuff
		// 这个检查是为了防止文档在滚动时出错
		if status, ok := docI["status"]; ok {
			if status.(int) == 404 {
				log.Error("error: ", docI["response"])
				continue
			}
		}

		// sanity check
		// 健康检查,字段是不是都正常存在
		for _, key := range []string{"_index", "_type", "_source", "_id"} {
			if _, ok := docI[key]; !ok {
				//json,_:=json.Marshal(docI)
				//log.Errorf("failed parsing document: %v", string(json))
				break READ_DOCS
			}
		}

		jsr,err:=json.Marshal(docI)　// 把对象转换为JSON
		log.Trace(string(jsr))
		if(err!=nil){
			log.Error(err)
		}
		n,err:=w.WriteString(string(jsr)) // 写入文件
		if(err!=nil){
			log.Error(n,err)
		}
		w.WriteString("\n")　// 写入换行符
		pb.Increment()　// 增加进度条

		// if channel is closed flush and gtfo
		// 如果通道关闭，flush和gtfo
		if !open {
			goto WORKER_DONE
		}
	}

	WORKER_DONE:
	w.Flush() // 完成所有数据写入后，客户端应调用 Flush 方法以确保所有数据已转发到基础 io.Writer。
	f.Close() // 关闭关闭文件，使其不能用于 I/O 

	wg.Done()　//  锁组-1
	log.Debug("file dump finished")
}


