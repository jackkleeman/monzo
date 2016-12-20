package main

// a speedy concurrent web crawler - written by Jack Kleeman for a monzo take home test
// jkleeman.me

import (
	"flag"
	"github.com/op/go-logging"
	"golang.org/x/net/html"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var log = logging.MustGetLogger("monzo")

type Page struct {
	URL     *url.URL
	Statics []*url.URL
	Links   []*Page
}

type SeenURLs struct {
	List  map[string]struct{} //valueless map, for checking if URL has already been seen
	Mutex sync.Mutex          //for threadsafe read and write access to the list
}

var wg sync.WaitGroup //this is a global waitgroup that is added to with every goroutine to prevent program end
var seenURLs SeenURLs //globally accessible, threadsafe seen URL list

func main() {
	var depth int
	var targetString string
	flag.StringVar(&targetString, "u", "http://www.jkleeman.me", "URL to start crawl on")
	flag.IntVar(&depth, "d", 5, "How deep the recursive crawler should search")
	flag.Parse()
	start := time.Now()
	targetURL, err := url.Parse(targetString)
	if err != nil {
		log.Error("couldn't parse that URL:", err)
		os.Exit(1)
	}
	seenURLs = SeenURLs{List: make(map[string]struct{})} //initialise the threadsafe array
	seenURLs.Mutex.Lock()                                //not exactly necessary, but good practice
	seenURLs.List[targetURL.String()] = struct{}{}
	seenURLs.Mutex.Unlock()
	target := Page{URL: targetURL} //create top level Page
	wg.Add(1)
	go crawlPage(&target, depth) //create first crawler goroutine
	wg.Wait()                    //this waits for every goroutine to finish
	elapsed := time.Since(start)
	printPage(&target, 0) //spit out the webmap
	log.Info("Unique links crawled:", len(seenURLs.List))
	log.Infof("Crawling took %s", elapsed)
}

func crawlPage(target *Page, depth int) error {
	defer wg.Done()
	if depth <= 0 { //reached our max depth
		return nil
	}
	resp, err := http.Get((*target).URL.String())
	if err != nil {
		log.Errorf("failed to get URL %s: %v", (*target).URL.String(), err)
		return err
	}
	defer resp.Body.Close()
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "text/html") { // "" to allow for no header being sent
		return nil
	}
	links := make(chan *Page)
	statics := make(chan *url.URL)
	var linkswg sync.WaitGroup //this is a page-local waitgroup to close links and statics channels when all parsing is done
	linkswg.Add(1)
	defer linkswg.Done() //allow static and links chans to close when this crawl ends
	wg.Add(1)
	go func() { //close static and links channels when parsing finishes
		defer wg.Done()
		linkswg.Wait()
		close(links)
		close(statics)
	}()
	wg.Add(1)
	go func() { //link collector
		defer wg.Done()
		for link := range links {
			(*target).Links = append((*target).Links, link)
		}
	}()
	wg.Add(1)
	go func() { //static collector
		defer wg.Done()
		for static := range statics {
			(*target).Statics = append((*target).Statics, static)
		}
	}()
	seenRefs := make(map[string]struct{}) //this will ensure we dont repeat the same statics and links within a given page
	tokens := html.NewTokenizer(resp.Body)
	for {
		tokenType := tokens.Next()
		if tokenType == html.ErrorToken { //an EOF
			return nil
		}
		token := tokens.Token()
		if tokenType == html.StartTagToken { //opening tag
			switch token.DataAtom.String() {
			case "a", "link": //link tags
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						_, ok := seenRefs[attr.Val]
						if !ok {
							seenRefs[attr.Val] = struct{}{} //add this ref to list of those seen on this page
							linkswg.Add(1)                  //linkswg stops the returning channel from closing
							go parseLink(attr.Val, target, links, &linkswg, depth)
						}
					}
				}
			case "img", "image", "script": //static tags
				for _, attr := range token.Attr {
					if attr.Key == "src" {
						_, ok := seenRefs[attr.Val]
						if !ok {
							seenRefs[attr.Val] = struct{}{} //add this ref to list of those seen on this page
							linkswg.Add(1)                  //linkswg stops the returning channel from closing
							go parseStatic(attr.Val, target, statics, &linkswg)
						}
					}
				}
			}
		}
	}
}

func parseLink(href string, current *Page, result chan *Page, waitgroup *sync.WaitGroup, depth int) error {
	defer (*waitgroup).Done()
	relURL, err := url.Parse(href)
	if err != nil {
		log.Errorf("failed to parse URL %s on page %s: %v", href, (*current).URL.String(), err)
		return err
	}
	newURL := (*current).URL.ResolveReference(relURL) //resolve the relative link to absolute
	if newURL.Host != (*current).URL.Host {           //we are not interested in external links
		return nil
	}
	newURL.Fragment = ""  //ignore fragments as they are irrelevant to crawling
	seenURLs.Mutex.Lock() //this blocks until no one else is writing to seenurls
	_, ok := seenURLs.List[newURL.String()]
	if ok { //this means we have seen this url before
		seenURLs.Mutex.Unlock()
		//result <- &newPage //give the page pointer back to main - even if the page has been seen before, we note but do not follow
		return nil
	}
	seenURLs.List[newURL.String()] = struct{}{} //add url to list of those seen
	seenURLs.Mutex.Unlock()
	newPage := Page{URL: newURL}
	wg.Add(1)
	go crawlPage(&newPage, depth-1) //recursively crawl the new page
	result <- &newPage
	return nil
}

func parseStatic(href string, current *Page, result chan *url.URL, waitgroup *sync.WaitGroup) error {
	defer (*waitgroup).Done()
	relURL, err := url.Parse(href)
	if err != nil {
		log.Errorf("failed to parse URL %s on page %s: %v", href, (*current).URL.String(), err)
		return err
	}
	/*if relURL.Host != (*current).URL.Host { //we are not interested in external links
		return nil
	}*/
	newURL := (*current).URL.ResolveReference(relURL) //resolve the link to absolute (ignores if it already was)
	newURL.Fragment = ""                              //ignore fragments as they are irrelevant to crawling
	result <- newURL                                  //give the URL pointer back to the main thread
	return nil
}

func printPage(page *Page, indent int) {
	a := strings.Join([]string{strings.Repeat("    ", indent), (*page).URL.String()}, "")
	log.Info(a)
	if len((*page).Statics) > 0 {
		b := strings.Join([]string{strings.Repeat("    ", indent+1), "Statics:"}, "")
		log.Info(b)
		for _, static := range (*page).Statics {
			c := strings.Join([]string{strings.Repeat("    ", indent+2), (*static).String()}, "")
			log.Info(c)
		}
	}
	if len((*page).Links) > 0 {
		d := strings.Join([]string{strings.Repeat("    ", indent+1), "Links:"}, "")
		log.Info(d)
		for _, subpage := range (*page).Links {
			printPage(subpage, indent+2)
		}
	}
}
