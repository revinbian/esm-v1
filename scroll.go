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
	"gopkg.in/cheggaaa/pb.v1" //简单的控制台进度条
	"encoding/json" // json相关的处理
	log "github.com/cihub/seelog" // 本地日志库
)


type ScrollAPI interface{
	GetScrollId()string　// 获取ScrollId
	GetHitsTotal()int　// 获取查找到的数量
	GetDocs() []interface{}　// 获取查找到的文档
	ProcessScrollResult(c *Migrator, bar *pb.ProgressBar)　// 处理获取到的数据
	Next(c *Migrator, bar *pb.ProgressBar) (done bool)　　// 根据ScrollId获取获取下一次的数据
}

// 下面都是Scroll结构体类实现，实现了ScrollAPI接口的所有方法

/**
* 获取查询到的数据数量
*/
func (scroll *Scroll) GetHitsTotal()int{
	//fmt.Println("total v0:",scroll.Hits.Total)
	return scroll.Hits.Total
}

/**
* 获取结构体中ScrollId的值
*/
func (scroll *Scroll) GetScrollId()string{
	return scroll.ScrollId
}

/**
* 获取结构体中查询到的文档
*/
func (scroll *Scroll) GetDocs()[]interface{}{
	//fmt.Println("docs v0:",scroll.Hits)
	return scroll.Hits.Docs
}

/**
* 获取查询到的数据数量－V7版本
*/
func (scroll *ScrollV7) GetHitsTotal()int{
	//fmt.Println("total v7:",scroll.Hits.Total.Value)
	return scroll.Hits.Total.Value
}

/**
* 获取结构体中ScrollId的值－V7版本
*/
func (scroll *ScrollV7) GetScrollId()string{
	return scroll.ScrollId
}

/**
* 获取结构体中查询到的文档－V7版本
*/
func (scroll *ScrollV7) GetDocs()[]interface{}{
	//fmt.Println("docs v7:",scroll.Hits)
	return scroll.Hits.Docs
}


// Stream from source es instance. "done" is an indicator that the stream is
// over
// 来自源es实例的流。““完成”表示流结束
func (s *Scroll) ProcessScrollResult(c *Migrator, bar *pb.ProgressBar)　{

	//update progress bar　更新处理进度条
	bar.Add(len(s.Hits.Docs))

	// show any failures　获取获取的一些信息
	for _, failure := range s.Shards.Failures {
		reason, _ := json.Marshal(failure.Reason)
		log.Errorf(string(reason))
	}

	// write all the docs into a channel　将所有的文档写入到通道中
	for _, docI := range s.Hits.Docs {
		//fmt.Println(docI)
		c.DocChan <- docI.(map[string]interface{})　//断言是不是map类型的,往通道里写数据
	}
}

// 根据ScrollId获取下一次的数据，并使用ProcessScrollResult方法数据，并且更新结构体里的ScrollId，用于下次
func (s *Scroll) Next(c *Migrator, bar *pb.ProgressBar) (done bool) {

	scroll,err:=c.SourceESAPI.NextScroll(c.Config.ScrollTime,s.ScrollId)
	if err != nil {
		log.Error(err)
		return false
	}

	docs:=scroll.(ScrollAPI).GetDocs()
	if docs == nil || len(docs) <= 0 {
		log.Debug("scroll result is empty")
		return true
	}

	scroll.(ScrollAPI).ProcessScrollResult(c,bar)

	//update scrollId
	s.ScrollId=scroll.(ScrollAPI).GetScrollId()

	return
}



// Stream from source es instance. "done" is an indicator that the stream is
// over
// 来自源es实例的流。““完成”表示流结束
func (s *ScrollV7) ProcessScrollResult(c *Migrator, bar *pb.ProgressBar)　{

	//update progress bar
	bar.Add(len(s.Hits.Docs))

	// show any failures
	for _, failure := range s.Shards.Failures {
		reason, _ := json.Marshal(failure.Reason)
		log.Errorf(string(reason))
	}

	// write all the docs into a channel
	for _, docI := range s.Hits.Docs {
		//fmt.Println(docI)
		c.DocChan <- docI.(map[string]interface{})
	}
}

// 根据ScrollId获取下一次的数据，并使用ProcessScrollResult方法数据，并且更新结构体里的ScrollId，用于下次
func (s *ScrollV7) Next(c *Migrator, bar *pb.ProgressBar) (done bool) {

	scroll,err:=c.SourceESAPI.NextScroll(c.Config.ScrollTime,s.ScrollId)
	if err != nil {
		log.Error(err)
		return false
	}

	docs:=scroll.(ScrollAPI).GetDocs()
	if docs == nil || len(docs) <= 0 {
		log.Debug("scroll result is empty")
		return true
	}

	scroll.(ScrollAPI).ProcessScrollResult(c,bar)

	//update scrollId
	s.ScrollId=scroll.(ScrollAPI).GetScrollId()

	return
}


