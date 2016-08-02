package main

import (
	"encoding/json"
	"fmt"

	"io/ioutil"
	_ "log"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
	"github.com/gin-gonic/gin"
	_ "github.com/semua/jiebago/tokenizers"
)

var IndexPool map[string]bleve.Index

func main() {
	IndexPool = make(map[string]bleve.Index)
	r := gin.Default()
	r.POST("/api/search/:index", Search)
	r.GET("/api/doc/:index/:docId", Doc)
	r.POST("/api/index/:index/:docId", Index)
	r.PUT("/api/update/:index/:docId", Index)
	r.DELETE("/api/delete/:index/:docId", Delete)
	r.Run(":9089")

	defer func() {
		for _, index := range IndexPool {
			if index != nil {
				index.Close()
			}
		}
	}()

}

func NewMapping() *bleve.IndexMapping {
	mapping := bleve.NewIndexMapping()
	err := mapping.AddCustomTokenizer("jieba",
		map[string]interface{}{
			"file":   "jieba_dict/dict.txt",
			"type":   "jieba",
			"hmm":    true,
			"search": true,
		})
	if err != nil {
		fmt.Println("AddCustomTokenizer error:", err)
	}
	err = mapping.AddCustomAnalyzer("jieba",
		map[string]interface{}{
			"type":      custom_analyzer.Name,
			"tokenizer": "jieba",
			"token_filters": []string{
				"possessive_en",
				"to_lower",
				"stop_en",
			},
		})
	if err != nil {
		fmt.Println("AddCustomAnalyzer jieba error:", err)
	}
	mapping.DefaultAnalyzer = "jieba"
	return mapping
}

func Index(c *gin.Context) {
	indexName := c.Params.ByName("index")
	_, ok := IndexPool[indexName]
	if !ok {
		index, err := bleve.Open(indexName)
		if err != nil {
			index, err = bleve.New(indexName, NewMapping())
			if err != nil {
				c.JSON(400, gin.H{"status": "Opening index error"})
				return
			}
		}
		IndexPool[indexName] = index
	}

	docId := c.Params.ByName("docId")
	if docId == "" {
		c.JSON(400, gin.H{"status": "Missing id"})
		return
	}

	var form map[string]interface{}

	requestBody, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error reading body"})
		return
	}
	fmt.Println(string(requestBody))
	err = json.Unmarshal(requestBody, &form)

	if err != nil {
		c.JSON(400, gin.H{"status": "Malformed Payload JSON"})
		return
	}

	err = IndexPool[indexName].Index(docId, form)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error indexing document"})
		return
	}

	c.JSON(200, gin.H{"status": "ok"})
}

func Search(c *gin.Context) {
	indexName := c.Params.ByName("index")
	_, ok := IndexPool[indexName]
	if !ok {
		index, err := bleve.Open(indexName)
		if err != nil {
			c.JSON(400, gin.H{"status": "Error opening index"})
			return
		}
		IndexPool[indexName] = index
	}

	requestBody, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error reading body"})
		return
	}

	var searchRequest bleve.SearchRequest
	fmt.Println(string(requestBody))
	err = json.Unmarshal(requestBody, &searchRequest)

	if err != nil {
		fmt.Println(err)
		c.JSON(400, gin.H{"status": "Error parsing query"})
		return
	}

	err = searchRequest.Query.Validate()
	if err != nil {
		c.JSON(400, gin.H{"status": "Error validating query"})
		return
	}

	searchResponse, err := IndexPool[indexName].Search(&searchRequest)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error executing the query"})
		return
	}

	reply := make([]string, 0)
	for _, r := range searchResponse.Hits {
		reply = append(reply, r.ID)
	}
	c.JSON(200, gin.H{"status": reply})
}

func Delete(c *gin.Context) {
	indexName := c.Params.ByName("index")
	_, ok := IndexPool[indexName]
	if !ok {
		index, err := bleve.Open(indexName)
		if err != nil {
			c.JSON(400, gin.H{"status": "Error opening index"})
			return
		}
		IndexPool[indexName] = index
	}
	docId := c.Params.ByName("docId")
	if docId == "" {
		c.JSON(400, gin.H{"status": "Missing id"})
		return
	}

	err := IndexPool[indexName].Delete(docId)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error deleting document"})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}
func Doc(c *gin.Context) {
	indexName := c.Params.ByName("index")
	_, ok := IndexPool[indexName]
	if !ok {
		index, err := bleve.Open(indexName)
		if err != nil {
			c.JSON(400, gin.H{"status": "Error opening index"})
			return
		}
		IndexPool[indexName] = index
	}
	docId := c.Params.ByName("docId")
	if docId == "" {
		c.JSON(400, gin.H{"status": "Missing id"})
		return
	}
	doc, err := IndexPool[indexName].Document(docId)
	if err != nil {
		c.JSON(400, gin.H{"status": "Error opening document"})
		return
	}
	jsonObj := simplejson.New()
	for _, field := range doc.Fields {
		jsonObj.Set(field.Name(), string(field.Value()))
	}
	response, _ := jsonObj.Map()
	c.JSON(200, response)
}
