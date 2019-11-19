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
	"sync"//Sync包同步提供基本的同步原语，如互斥锁
	log "github.com/cihub/seelog"  // 日志库
	"encoding/json"
	"bytes"
	"gopkg.in/cheggaaa/pb.v1"  // 简单的控制台进度条
	"time"
	"strings"
)

func (c *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf) // NewEncoder返回一个写入w的新编码器

	READ_DOCS:
	for {
		select {
		case docI, open := <-c.DocChan: // 从通道中获取数据
			var err error
			log.Trace("read doc from channel,", docI)
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
					break READ_DOCS //有问题则进行下一行
				}
			}

			var tempDestIndexName string
			var tempTargetTypeName string
			tempDestIndexName = docI["_index"].(string)　// 目标es索引, 使用文档中的
			tempTargetTypeName = docI["_type"].(string)　// 目标es　type, 使用文档中的

			if c.Config.TargetIndexName != "" {
				tempDestIndexName = c.Config.TargetIndexName //　使用配置中的
			}

			if c.Config.OverrideTypeName != "" {
				tempTargetTypeName = c.Config.OverrideTypeName　//　使用配置中的
			}


			// 组成一个新的文档
			doc := Document{
				Index:  tempDestIndexName,
				Type:   tempTargetTypeName,
				source: docI["_source"].(map[string]interface{}),　// 断言
				Id:     docI["_id"].(string),　// 断言类型
			}


			if c.Config.RenameFields != "" {　// 重新修改字段名字
				kvs:=strings.Split(c.Config.RenameFields,",")
				//fmt.Println(kvs)
				for _,i:=range kvs{
					fvs:=strings.Split(i,":")
					oldField:=strings.TrimSpace(fvs[0])
					newField:=strings.TrimSpace(fvs[1])
					if oldField=="_type"{
						doc.source[newField]=docI["_type"].(string)
					}else{
						v:=doc.source[oldField]
						doc.source[newField]=v
						delete(doc.source,oldField)
					}
				}
			}


			//fmt.Println(doc.Index,",",doc.Type,",",doc.Id)

			// add doc "_routing"
			// 添加　doc "_routing"　值
			if _, ok := docI["_routing"]; ok {
				str,ok:=docI["_routing"].(string)
				if ok && str!=""{
					doc.Routing =str
				}
			}

		// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

		// sanity check
			if len(doc.Index) == 0 || len(doc.Id) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

		// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"index": doc,
			}
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

		// if we approach the 100mb es limit, flush to es and reset mainBuf
		// 如果接近100mb es限制，刷新至es并重置mainBuf
			if mainBuf.Len() + docBuf.Len() > (c.Config.BulkSizeInMB * 1000000) {
				goto CLEAN_BUFFER
			}

		// append the doc to the main buffer
			mainBuf.Write(docBuf.Bytes())
		// reset for next document
			bulkItemSize++
			docBuf.Reset()
			(*docCount)++
		case <-time.After(time.Second * 5):
			log.Debug("5s no message input")
			goto CLEAN_BUFFER
		case <-time.After(time.Minute * 5):
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}

		goto READ_DOCS

		CLEAN_BUFFER:
		c.TargetESAPI.Bulk(&mainBuf)
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(bulkItemSize)
		bulkItemSize = 0

	}
	WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
		bulkItemSize++
	}
	c.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	wg.Done()
}
