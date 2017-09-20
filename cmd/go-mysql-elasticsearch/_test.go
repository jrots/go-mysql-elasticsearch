package main

import (
	"fmt"
	"time"
	"context"
)

func main() {
	client, err := elastic.NewClient(
		elastic.SetURL(	"http://elastic30:9200",
			  			"http://elastic31:9200"))
	fmt.Println(client)
	fmt.Println(err)

	service, _ := client.BulkProcessor().Name("MyBackgroundWorker-1").
	 	Workers(2).
    	BulkActions(1000).              // commit if # requests >= 1000
    	BulkSize(2 << 20).              // commit if size of requests >= 2 MB
    	FlushInterval(30*time.Second).  // commit every 30s
	Do(context.Background())

	doc := map[string]interface{}{
						"retweets" : 43,
	}
	update2Req := elastic.NewBulkUpdateRequest().Index("twoo_prod_1").Type("doc").Id("3").
		RetryOnConflict(2).DocAsUpsert(true).
                Doc(doc)
	fmt.Println(update2Req)
	service.Add(update2Req)
	service.Flush()

	stats := service.Stats()

fmt.Printf("Number of times flush has been invoked: %d\n", stats.Flushed)
fmt.Printf("Number of times workers committed reqs: %d\n", stats.Committed)
fmt.Printf("Number of requests indexed            : %d\n", stats.Indexed)
fmt.Printf("Number of requests reported as created: %d\n", stats.Created)
fmt.Printf("Number of requests reported as updated: %d\n", stats.Updated)
fmt.Printf("Number of requests reported as success: %d\n", stats.Succeeded)
fmt.Printf("Number of requests reported as failed : %d\n", stats.Failed)

}