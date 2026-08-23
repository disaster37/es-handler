package eshandler

import (
	"net/http"
	"testing"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	"github.com/jarcoal/httpmock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

const baseURL = "http://localhost:9200"

type ElasticsearchHandlerTestSuite struct {
	suite.Suite
	esHandler ElasticsearchHandler
}

func TestElasticsearchHandlerSuite(t *testing.T) {
	suite.Run(t, new(ElasticsearchHandlerTestSuite))
}

func (t *ElasticsearchHandlerTestSuite) SetupTest() {
	// Order is critical: elasticsearch.New clones http.DefaultTransport with a
	// hard *http.Transport assertion, so New MUST run BEFORE httpmock.Activate().
	cfg := &elasticsearch.Config{URL: baseURL}
	client, err := elasticsearch.New(cfg, logrus.NewEntry(logrus.New()))
	if err != nil {
		panic(err)
	}
	// Point the underlying resty client at httpmock's transport AFTER New().
	// httpmock.DefaultTransport (*httpmock.MockTransport) implements http.RoundTripper.
	client.RestyClient().SetTransport(httpmock.DefaultTransport)

	t.esHandler = &ElasticsearchHandlerImpl{
		client: client,
		log:    logrus.NewEntry(logrus.New()),
	}

	httpmock.Activate() // now safe: http.DefaultTransport is no longer used by our client
}

func (t *ElasticsearchHandlerTestSuite) BeforeTest(suiteName, testName string) {
	httpmock.Reset()
}

func (t *ElasticsearchHandlerTestSuite) TearDownTest() {
	// Restore http.DefaultTransport so the next test's elasticsearch.New() can
	// clone the real *http.Transport instead of httpmock's *MockTransport.
	httpmock.Deactivate()
}

func SetHeaders(resp *http.Response) { // optional now (X-Elastic-Product is only a DEBUG log)
	resp.Header.Add("X-Elastic-Product", "Elasticsearch")
}
