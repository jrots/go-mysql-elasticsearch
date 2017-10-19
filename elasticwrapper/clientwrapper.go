package elasticwrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
	elastic "gopkg.in/olivere/elastic.v6"
//	"os"
//	"strings"
)

// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Addr          string
	User          string
	Password      string
//	File		*os.File
	BulkProcessor *elastic.BulkProcessor
	BulkProcessorDelete *elastic.BulkProcessor

	totalRequests int
	c    *elastic.Client
}

type ClientConfig struct {
	Addr     string
	User     string
	Password string
}

// after is invoked by bulk processor after every commit.
// The err variable indicates success or failure.
func (c *Client) after(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
			fmt.Println(err);
	}
	//fmt.Println(response.Took, response.Errors, len(response.Items))
}


func NewClient(conf *ClientConfig) *Client {

	c := new(Client)
/*
	updates, err := os.OpenFile("./updatesfile", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
    	panic(err)
	}
	c.File = updates
*/
	c.Addr = conf.Addr
	c.User = conf.User
	c.Password = conf.Password
	client, err := elastic.NewClient(
		elastic.SetURL(	c.Addr ))

	if err != nil {
		panic(err)
	}
	c.c = client
	c.totalRequests = 0
	bulk, err := c.c.BulkProcessor().Name("MyBackgroundWorker-1").
		Workers(1).
		BulkActions(75).               // commit if # requests >= 1000
		BulkSize(40 << 20).               // commit if size of requests >= 2 MB
		FlushInterval(120 * time.Second). // commit every 30s
		After(c.after).
		Do(context.Background())
	if err == nil {
		c.BulkProcessor = bulk
	}
	/*
	bulkDel, err := c.c.BulkProcessor().Name("DeleteWorker-1").
		Workers(2).
		BulkActions(10).               // commit if # requests >= 1000
		BulkSize(5 << 20).               // commit if size of requests >= 2 MB
		FlushInterval(20 * time.Second). // commit every 10s
		After(c.after).
		Do(context.Background())
	if err == nil {
		c.BulkProcessorDelete = bulkDel
	}*/

	return c
}

type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

type Response struct {
	Code int
	ResponseItem
}

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

type BulkRequest struct {
	Action        string
	Index         string
	Type          string
	ID            string
	Parent        string
	JoinField     string
	JoinFieldName string

	HardCrud bool
	Initial bool

	Data         map[string]interface{}
	DeleteFields map[string]interface{}
}

func (r *BulkRequest) prepareBulkUpdateRequest() (*elastic.BulkUpdateRequest, error) {

	bulkRequest := elastic.NewBulkUpdateRequest()
	/*update2Req := elastic.NewBulkUpdateRequest().Index("twoo_prod_1").Type("doc").Id("3").
	RetryOnConflict(2).DocAsUpsert(true).
	Doc(doc)
	*/
	if len(r.Index) > 0 {
		bulkRequest.Index(r.Index)
	}
	if len(r.Type) > 0 {
		bulkRequest.Type(r.Type)
	}

	if len(r.ID) > 0 {
		bulkRequest.Id(r.ID)
	}
	if len(r.JoinField) > 0 {
		if len(r.Parent) > 0 {
			r.Data[r.JoinField] = map[string]interface{}{
				"name":   r.JoinFieldName,
				"parent": r.Parent,
			}
			bulkRequest.Routing(r.Parent)
		} else if r.Initial {
			r.Data[r.JoinField] = map[string]interface{}{
				"name": r.JoinFieldName,
			}
		}
	} else if len(r.Parent) > 0 {
		bulkRequest.Parent(r.Parent)
	}
	if r.Action == ActionUpdate || !r.HardCrud {
		bulkRequest.RetryOnConflict(2)
	}
	/* @TODO fix hardcrud seperate actions!
	if r.HardCrud {
		meta[r.Action] = metaData
	} else {
		meta["update"] = metaData // all requests are update in this case
	}
	*/

	doc := map[string]interface{}{}

	switch r.Action {
	case ActionDelete:
		if !r.HardCrud {
			var del bytes.Buffer
			del.WriteString("for (entry in params.entrySet()) { ctx._source.remove(entry.getKey()) }")
			bulkRequest.Script(elastic.NewScriptInline(del.String()).Type("source").Lang("painless").Params(r.Data))
			return bulkRequest, nil
		}
	case ActionUpdate:
		// When more then 1 item to update
		// When no parent and not initial data
		if len(r.Data) > 1 || (len(r.Parent) == 0 && len(r.Data) == 1 && !r.Initial)  {
			doc = r.Data
		}

	default:

		doc = r.Data
	}

	if len(doc) > 0 {
		bulkRequest.DocAsUpsert(true)
		return bulkRequest.Doc(doc), nil
	} else {
		return bulkRequest, errors.New("empty update")
	}
}

type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

type BulkResponseItem struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Status  int             `json:"status"`
	Error   json.RawMessage `json:"error"`
	Found   bool            `json:"found"`
}

func (c *Client) DoRequest(method string, url string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(c.User) > 0 && len(c.Password) > 0 {
		req.SetBasicAuth(c.User, c.Password)
	}

	req.Header.Set("Content-Type", "application/json")
//	resp, err := c.c.Do(req)
	//@TODO fix
	return &http.Response{}, err
}

func (c *Client) Do(method string, url string, body map[string]interface{}) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)

	resp, err := c.DoRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(Response)
	ret.Code = resp.StatusCode

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret.ResponseItem)
	}

	return ret, errors.Trace(err)
}

func (c *Client) OutputStats() {

	stats := c.BulkProcessor.Stats()

	fmt.Printf("Number of times flush has been invoked: %d\n", stats.Flushed)
	fmt.Printf("Number of times workers committed reqs: %d\n", stats.Committed)
	fmt.Printf("Number of requests indexed            : %d\n", stats.Indexed)
	fmt.Printf("Number of requests reported as created: %d\n", stats.Created)
	fmt.Printf("Number of requests reported as updated: %d\n", stats.Updated)
	fmt.Printf("Number of requests reported as success: %d\n", stats.Succeeded)
	fmt.Printf("Number of requests reported as failed : %d\n", stats.Failed)
}

func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var bulkRequest *elastic.BulkUpdateRequest
	var err error
	for _, item := range items {

		if bulkRequest, err = item.prepareBulkUpdateRequest(); err == nil {
			if item.Action == ActionDelete {
				c.totalRequests = c.totalRequests+1
				c.BulkProcessor.Add(bulkRequest)
			} else {
				c.totalRequests = c.totalRequests+1
				c.BulkProcessor.Add(bulkRequest)
			}
		}

		if len(item.DeleteFields) > 0 {
			for k := range item.DeleteFields {
				delReq := new(BulkRequest)
				delReq.Action = ActionDelete
				delReq.Type = item.Type
				delReq.ID = item.ID
				delReq.Index = item.Index
				delReq.Data = make(map[string]interface{})
				delReq.Data[k] = true

				if bulkRequest, err = delReq.prepareBulkUpdateRequest(); err == nil {
					c.BulkProcessor.Add(bulkRequest)
					c.totalRequests = c.totalRequests+1
				}
			}
		}
	}

	return &BulkResponse{}, nil
}

func (c *Client) CreateMapping(index string, docType string, mapping map[string]interface{}) error {
	reqUrl := fmt.Sprintf("http://%s/%s", c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("HEAD", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	// if index doesn't exist, will get 404 not found, create index first
	if r.Code == http.StatusNotFound {
		_, err = c.Do("PUT", reqUrl, nil)

		if err != nil {
			return errors.Trace(err)
		}
	} else if r.Code != http.StatusOK {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}

	reqUrl = fmt.Sprintf("http://%s/%s/%s/_mapping", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	_, err = c.Do("POST", reqUrl, mapping)
	return errors.Trace(err)
}

func (c *Client) DeleteIndex(index string) error {
	reqUrl := fmt.Sprintf("http://%s/%s", c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

func (c *Client) Get(index string, docType string, id string) (*Response, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	return c.Do("GET", reqUrl, nil)
}

// Can use Update to create or update the data
func (c *Client) Update(index string, docType string, id string, data map[string]interface{}) error {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqUrl, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

func (c *Client) Exists(index string, docType string, id string) (bool, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("HEAD", reqUrl, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

func (c *Client) Delete(index string, docType string, id string) error {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/_bulk", c.Addr)

	return c.DoBulk(reqUrl, items)
}

func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/_bulk", c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqUrl, items)
}

func (c *Client) IndexTypeBulk(index string, docType string, items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/_bulk", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	return c.DoBulk(reqUrl, items)
}
