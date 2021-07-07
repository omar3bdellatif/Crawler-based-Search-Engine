
import indexerPackage.DB_Connection;
import indexerPackage.Indexer;
import indexerPackage.Ranker;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler implements Runnable {

    private static class robotsDetailedInfo {
        public Set<String> allowed;
        public Set<String> disallowed;
        public robotsDetailedInfo()
        {
            allowed = new HashSet<>();
            disallowed = new HashSet<>();
        }
    }

    private static DB_Connection db;

    // Crawler
    private static int maxCrawls = 7500; //number of websites to crawl
    private static Set<URL> visited_urls;
    private static int maxThreads;
    private static int noOfThreads = 0;
    private URL tURL;
    private static WebCrawler wcr; // for synchronization
    private static Map<String, robotsDetailedInfo> robotsMap;
    private static Map<String, robotsDetailedInfo> robotsMapDefaultAgent;
    private static String myUserAgent = "*";
    private static boolean didFinishCrawler = false;
    private static int savedDocsSize = 0;


    @Override
    public void run() {
        Set<URL> crawlURLs = new HashSet<>();
        crawlURLs.add(this.tURL);
        crawl(crawlURLs);
    }

    // Only threads call this constructor
    public WebCrawler(URL tURL)
    {
        this.tURL = tURL;

        Thread.currentThread().setName(UUID.randomUUID().toString());
        synchronized (wcr)
        {
            noOfThreads += 1;
        }
    }


    // for Recrawl or initial crawl
    public WebCrawler(Set<URL> urls, int maxThreads)
    {
        db = new DB_Connection("crawlerX");
        wcr = this;
        this.maxThreads = maxThreads;
        visited_urls = new HashSet<>();

        didFinishCrawler = false;
        robotsMap = new HashMap<>();
        robotsMapDefaultAgent = new HashMap<>();

        for(URL item: urls)
        {
            Thread t = new Thread(new WebCrawler(item));
            t.start();
        }
    }


    // for resuming after stop
    public WebCrawler() throws MalformedURLException, SQLException {
        db = new DB_Connection("crawlerX");
        wcr = this;
        this.maxThreads = 20;
        visited_urls = db.getCrawledLinks();
        maxCrawls = maxCrawls-visited_urls.size();
        savedDocsSize = 0;
        didFinishCrawler = false;
        robotsMap = new HashMap<>();
        robotsMapDefaultAgent = new HashMap<>();

        Set<URL> urls = db.getNonCrawledLinks();
        Set<URL> backupSet = new HashSet<>();

        for(URL item: urls)
        {
            if (noOfThreads < maxThreads) {
                Thread t = new Thread(new WebCrawler(item));
                t.start();
            }
            else
            {
                backupSet.add(item);
            }
        }

        if (!backupSet.isEmpty())
            crawl(backupSet);

    }

    public void crawl(Set<URL> urls) {

        if (didFinishCrawler) {
            synchronized (wcr)
            {
                noOfThreads--;
            }
            return;
        }

        synchronized (wcr) {
            urls.removeAll(visited_urls); // remove the already visited urls from the urls to be crawled
            visited_urls.addAll(urls);
        }
        for (URL url : urls) {
            db.insertValueToWebLinks(url);
        } //add it to the database as not crawled yet

        if (urls.isEmpty()) {
            synchronized (wcr)
            {
                noOfThreads --;
            }
            return;
        }

        Set<URL> seedSet = new HashSet<>();
        URL currentURL;

        for (URL url : urls) {
            if (didFinishCrawler)
            {
                synchronized (wcr) {
                    noOfThreads--;
                }
                return;
            }
            currentURL = url;
            System.out.println("Currently Crawling: " + url.toString());
            Document doc;
            try {
                doc = Jsoup.connect(url.toString()).timeout(120000).get();
            }
            catch (Exception e)
            {
                continue;
            }


            synchronized (wcr) {
                savedDocsSize += 1;
            }
            if (savedDocsSize >= maxCrawls)
            {
                if (didFinishCrawler)
                {
                    synchronized (wcr) {
                        noOfThreads--;
                    }
                    return;
                }
                synchronized (wcr)
                {
                    didFinishCrawler = true;
                    noOfThreads--;
                }
                return;
            }
            db.updateCrawled(url.toString(), doc.title()); //update the crawled column to true and add it's title

            Elements pageURLs = doc.select("a[href]");
            for (Element element : pageURLs) {
                if (didFinishCrawler)
                    return;
                try {
                    String urlAsString = element.attr("abs:href");
                    //check for robots.txt
                    URL savedURL = new URL(urlAsString);
                    if(!isSiteAllowed(savedURL))
                    {
                        synchronized (wcr) {
                            visited_urls.add(savedURL);
                        }
                        if (didFinishCrawler)
                            return;

                        continue;
                    }

                    if (noOfThreads > maxThreads)
                    {
                        seedSet.add(new URL(urlAsString));
                    }
                    else
                    {
                        Thread t = new Thread(new WebCrawler(new URL(urlAsString)));
                        t.start();
                    }
                }
                catch (Exception e)
                {
                    if (didFinishCrawler)
                    {
                        synchronized (wcr) {
                            noOfThreads--;
                        }
                        return;
                    }

                    synchronized(wcr) {
                        visited_urls.add(currentURL);
                    }

                }
            }
        }

        if (!seedSet.isEmpty())
            crawl(seedSet);

        synchronized (wcr) {
            noOfThreads--;
        }

    }


    //robots function
    private robotsDetailedInfo fetchSiteRobot(URL url)
    {
        robotsDetailedInfo returned = new robotsDetailedInfo();

        String host = "https://" + url.getHost(); //without any extentions
        String robotsTxtURL = host + "/robots.txt";

        Document doc = null;
        try{
            doc = Jsoup.connect(robotsTxtURL).timeout(100000).get();
        }
        catch (Exception e)
        {
            return returned;
        }
        if (didFinishCrawler)
            return returned;

        Elements pageBody = doc.select("body");

        String body = "";

        for (Element el: pageBody)
        {
            body = el.getElementsByTag("body").first().text();
        }
        String originalBody = body;

        return this.getRobotsInfoForBody(originalBody, myUserAgent);
    }


    private boolean isSiteAllowed(URL url)
    {
        if (didFinishCrawler)
            return false;
        String host = "https://" + url.getHost();

        if (robotsMap.get(host) == null)
        {
            robotsDetailedInfo robotsTxtSites = this.fetchSiteRobot(url);
            robotsMap.put(host, robotsTxtSites);
        }

        Set<String> allowed_sites = new HashSet<>();
        Set<String> disallowed_sites = new HashSet<>();

        allowed_sites = robotsMap.get(host).allowed;
        disallowed_sites = robotsMap.get(host).disallowed;

        // Checking if the site matches any Allowed sites FIRST
        for(String sitePattern: allowed_sites)
        {
            Pattern patternCompiled = Pattern.compile(sitePattern);
            Matcher matcher = patternCompiled.matcher(url.getFile().toString());
            boolean didMatch = matcher.matches();
            if(didMatch)
            {
                return true;
            }
        }

        // Checking if the site matches any Disallowed sites
        for(String sitePattern: disallowed_sites)
        {
            Pattern patternCompiled = Pattern.compile(sitePattern);
            Matcher matcher = patternCompiled.matcher(url.getFile().toString());
            boolean didMatch = matcher.matches();
            if(didMatch)
            {
                return false;
            }
        }
        return true;
    }

    private robotsDetailedInfo getRobotsInfoForBody(String inBody, String userAgent)
    {
        String body = new String(inBody);
        int userAgent_LENGTH = 12;
        robotsDetailedInfo returnedRobotsInfo = new robotsDetailedInfo();
        returnedRobotsInfo.allowed = new HashSet<>();
        int agentInd = body.indexOf("User-agent: " + userAgent);

        if (agentInd == -1)
        {
            return returnedRobotsInfo;
        }
        else
        {
            body = body.substring(agentInd+userAgent_LENGTH+userAgent.length());
        }
        StringTokenizer bodyTokenizer = new StringTokenizer(body);

        while(bodyTokenizer.hasMoreTokens())
        {
            String type = bodyTokenizer.nextToken(); // Allowed or Disallowed

            if ((!(type.startsWith("Allow") || type.startsWith("Disallow:"))) || !bodyTokenizer.hasMoreTokens())
            {
                agentInd = body.indexOf("User-agent: " + userAgent);
                if (agentInd == -1)
                    break;
                else
                {
                    body = body.substring(agentInd+userAgent_LENGTH+userAgent.length());
                    bodyTokenizer = new StringTokenizer(body);
                    continue;
                }
            }

            String siteRegex = bodyTokenizer.nextToken();

            if (!siteRegex.startsWith("/"))  //if DisAllowed: blank
            {
                break;
            }
            // [a-zA-Z0-9_]*
            siteRegex = siteRegex.replaceAll("\\*", ".*");
            siteRegex = siteRegex.replaceAll("\\?", ".?");

            if (siteRegex.endsWith("/"))
            {
                siteRegex += ".*";
            }

            siteRegex = "^" + siteRegex + "$";

            if (type.equals("Disallow:"))
            {
                returnedRobotsInfo.disallowed.add(siteRegex);
            }
            else if (type.equals("Allow:"))
            {
                returnedRobotsInfo.allowed.add(siteRegex);
            }
            else
            {
                agentInd = body.indexOf("User-agent: " + userAgent);
                if (agentInd == -1)
                    break;
                else
                {
                    body = body.substring(agentInd+userAgent_LENGTH+userAgent.length());
                    bodyTokenizer = new StringTokenizer(body);
                }
            }
        }
        return returnedRobotsInfo;
    }


}
