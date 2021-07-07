package indexerPackage;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.tartarus.snowball.ext.englishStemmer;

public class Indexer {

    public static Map<String, Document> savedDocs;
    public static DB_Connection db;
    public static Map<String, String> stopwordsMap;
    public static final int MAX_THREADS = 8;
    public static int currentThreads = 0;
    public static Map<String, String> synonymsMap;
    private static final int PREVIEW_LENGTH = 50;

    private void setSynonymsMap()
    {
        // TODO: Add any other synonyms that may appear in the results
        synonymsMap = new HashMap<>();
        synonymsMap.put("mobil", "phone");
        synonymsMap.put("movi", "film");
        synonymsMap.put("intellig", "smart");
        synonymsMap.put("laptop", "computer");
        synonymsMap.put("program", "app");
        synonymsMap.put("imag", "photo");
        synonymsMap.put("coding", "code");

        synonymsMap.put("phone", "mobil");
        synonymsMap.put("film", "movi");
        synonymsMap.put("smart", "intellig");
        synonymsMap.put("comput", "laptop");
        synonymsMap.put("app", "program");
        synonymsMap.put("photo", "imag");
        synonymsMap.put("code", "coding");
    }

    public Indexer()
    {

    }


    public Map<String, String> indexedURLs;
    public Indexer(DB_Connection dbIn) {
        stopwordsMap = new HashMap<>();
        db = dbIn;
        ArrayList<DB_Connection.WebLink> crawlerOutput = db.getLinks();

        setLocalStopWordsMap();
        setSynonymsMap();


        indexedURLs = new HashMap<>();
        try {
            indexedURLs = db.getIndexedURLs();
        } catch (SQLException ex) {
            System.out.println(ex.getLocalizedMessage());
            return;
        }

        List<String> unIndexedCrawlerOutput = new ArrayList<>();

        for (DB_Connection.WebLink c: crawlerOutput)
        {
            if (indexedURLs.get(c.url) != null)
                continue;
            unIndexedCrawlerOutput.add(c.url);
        }

        int remainingURLs = unIndexedCrawlerOutput.size();
        int URLsPerThread = remainingURLs / MAX_THREADS;
        for(int i=0; i<MAX_THREADS-1; i++)
        {
            List<String> currentCrawlerOutput;
            currentCrawlerOutput = unIndexedCrawlerOutput.subList(i*URLsPerThread,((i+1)*URLsPerThread));
            new IndexerThread(currentCrawlerOutput).start();
        }
        int lastIndexedURLIndex = (MAX_THREADS - 1) * URLsPerThread;
        List<String> currentCrawlerOutput = unIndexedCrawlerOutput.subList(lastIndexedURLIndex, unIndexedCrawlerOutput.size());
        new IndexerThread(currentCrawlerOutput).start();

    }


    public static class IndexerThread extends Thread
    {
        List<String> urls;
        public IndexerThread(List<String> inURLs)
        {
            this.urls = inURLs;
        }

        @Override
        public void run() {
            for (String url: this.urls) {

                Document doc;
                try {
                    doc = Jsoup.connect(url).timeout(100000).get();
                } catch (Exception e) {
                    continue;
                }
                doIndexerWork(url, doc);
            }
        }
    }

    public Indexer(Map<String, Document> savedDocsIn, DB_Connection dbIn)
    {
        System.out.println("Saved Docs Size = " + savedDocsIn.size());
        stopwordsMap = new HashMap<>();
        db = dbIn;
        savedDocs = savedDocsIn;
        setLocalStopWordsMap();
        setSynonymsMap();

        for (String urlString: savedDocs.keySet())
        {
            System.out.println(urlString);
            boolean flag = true;
            Map<String, String> indexedURLs = new HashMap<>();
            try {
                indexedURLs = db.getIndexedURLs();
            } catch (SQLException ex) {
                System.out.println(ex.getLocalizedMessage());
                continue;
            }
            if(indexedURLs.get(urlString) != null){
                flag = false;
            }
            Document doc = savedDocs.get(urlString);
            doIndexerWork(urlString, doc);
        }
    }

    public static void doIndexerWork(String urlString, Document doc)
    {
        Map<String, Word>wordsMap = new HashMap<>();

        int wordsCount = 1000;


        try {
            wordsCount = quickGetCount(wordsMap, urlString, doc);
        } catch (Exception e) {

            return;
        }

        try {
            URL url = new URL(urlString);
            quickIndexerModule(wordsMap, url, doc);
        } catch (Exception e) {
           return;
        }

        String date = getPubDate(doc);
        String location = "Global";
        try{
            location = getPageLocation(urlString);
        }
        catch (MalformedURLException e){
            location = "Global";
        }

        dbInsertionCompletion(urlString, wordsMap, db, date, location, wordsCount);

    }



    public void setLocalStopWordsMap()
    {
        String stopWords[] ={"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};
        for(int i=0;i<stopWords.length;i++){
            this.stopwordsMap.put(stopWords[i], stopWords[i]);
        }
    }

    public static void setStopWords(Map<String,String> stopwordsMap){
        String stopWords[] ={"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};
        for(int i=0;i<stopWords.length;i++){
            stopwordsMap.put(stopWords[i],stopWords[i]);
        }
    }

    public static void getTagCount(String tag,Document doc,Map<String,Word>wordsMap,Map<String, String>stopwordsMap){
        Elements link = doc.select(tag);

        for (Element element : link) {

            String word = element.text();
            String[] arr=word.split("[\\s\t\n]+");

            for(int i=0;i<arr.length;i++)
            {
                String skippingSpecialChars = arr[i].replaceAll("[0-9–.©+=:_#$%^&*~'’,()\\?\\/*@<>{}|\"\\\\!;-]+"," ");

                String[] arr2 = skippingSpecialChars.split(" ");
                for(int j=0;j<arr2.length;j++){
                    if (arr2[j].matches("[ \t\n]*")) {
                        continue;
                    }
                    if(arr2[j].matches("(.*)[^a-zA-Z]+(.*)")){
                        continue;
                    }

                    String caseInsensitive = arr2[j].toLowerCase();
                    String stemmed = StemWords(caseInsensitive);
                    if(stopwordsMap.get(stemmed) != null){
                        continue;
                    }
                    Word wordObj = wordsMap.get(stemmed);
                    if (wordObj == null) {
                        wordObj = new Word();
                        wordObj.word = stemmed;
                        if(tag.equals("h1")){
                            wordObj.h1Count = 1;
                        }
                        else if (tag.equals("h2")){
                            wordObj.h2Count = 1;
                        }
                        else if (tag.equals("h3")){
                            wordObj.h3Count = 1;
                        }
                        else if(tag.equals("h4")){
                            wordObj.h4Count = 1;
                        }
                        else if(tag.equals("h5")){
                            wordObj.h5Count = 1;
                        }
                        else if(tag.equals("h6")){
                            wordObj.h6Count = 1;
                        }
                        else if (tag.equals("i")){
                            wordObj.ItalicCount = 1;
                        }
                        else if (tag.equals("b")){
                            wordObj.BoldCount = 1;
                        }
                        else if(tag.equals("title")){
                            wordObj.TitleCount = 1;
                            wordObj.count++;
                        }

                        wordsMap.put(stemmed, wordObj);
                    }
                    else{

                        if(tag.equals("h1")){
                            wordObj.h1Count++;
                        }
                        else if (tag.equals("h2")){
                            wordObj.h2Count++;
                        }
                        else if (tag.equals("h3")){
                            wordObj.h3Count++;
                        }
                        else if (tag.equals("h4")){
                            wordObj.h4Count++;
                        }
                        else if (tag.equals("h5")){
                            wordObj.h5Count++;
                        }
                        else if (tag.equals("h6")){
                            wordObj.h6Count++;
                        }
                        else if (tag.equals("i")){
                            wordObj.ItalicCount++;
                        }
                        else if (tag.equals("b")){
                            wordObj.BoldCount++;
                        }else if(tag.equals("title")){
                            wordObj.TitleCount++;
                            wordObj.count++;
                        }
                    }
                }

            }
        }
    }

    public static void quickIndexerModule(Map<String,Word>wordsMap, URL url, Document doc)
    {
        getTagCount("h1",doc,wordsMap,stopwordsMap);
        getTagCount("b",doc,wordsMap,stopwordsMap);
        getTagCount("h2",doc,wordsMap,stopwordsMap);
        getTagCount("h3",doc,wordsMap,stopwordsMap);
        getTagCount("h4",doc,wordsMap,stopwordsMap);
        getTagCount("h5",doc,wordsMap,stopwordsMap);
        getTagCount("h6",doc,wordsMap,stopwordsMap);
        getTagCount("i",doc,wordsMap,stopwordsMap);
        getTagCount("title",doc,wordsMap,stopwordsMap);
        updateImageSrc(doc, wordsMap, url);
    }

    private static void updateImageSrc(Document doc, Map<String, Word> wordsMap, URL url)
    {
        for (String stemmed: wordsMap.keySet())
        {
            Word tempW = wordsMap.get(stemmed);
            tempW.imageSrc = getImageFromDoc(doc, stemmed, url);
            if (tempW.imageSrc.equals(""))
            {
                if (synonymsMap.containsKey(stemmed))
                    tempW.imageSrc = getImageFromDoc(doc, synonymsMap.get(stemmed), url);
            }

            wordsMap.put(stemmed, tempW);
        }
    }



    public static String StemWords(String word){
        englishStemmer stemmer = new englishStemmer();
        stemmer.setCurrent(word);
        stemmer.stem();
        return stemmer.getCurrent();
    }


    public static int quickGetCount(Map<String, Word> wordsMap, String url, Document doc) throws Exception
    {
        String text = doc.body().text();

        String line = text;

        int index = 0;

        String[] arr = line.split("[\\s\t\n]+");
        for (String s : arr) {
            String skippingSpecialChars = s.replaceAll("[0-9–.©+=:_#$%^&*~'’,()\\?\\/*@<>{}|\"\\\\!;-]+", " ");

            String[] arr2 = skippingSpecialChars.split(" ");
            for (String value : arr2) {
                if (value.matches("[ \t\n]*")) {
                    continue;
                }
                if (value.matches("(.*)[^a-zA-Z]+(.*)")) {
                    continue;
                }
                String caseInsensitive = value.toLowerCase();
                String stemmed = StemWords(caseInsensitive);

                if (stopwordsMap.get(stemmed) != null) {
                    //index++;
                    continue;
                }
                Word wordObj = wordsMap.get(stemmed);
                if (wordObj == null) {
                    wordObj = new Word();
                    wordObj.word = stemmed;
                    wordObj.count = 1;
                    wordObj.bodyPrev = getBodyPreviewFromDoc(doc, stemmed, line);
                    index++;
                    wordsMap.put(stemmed, wordObj);
                } else {
                    wordObj.count++;
                    index++;
                }
            }
        }
        return index;
    }
    public static class Word {
        public String word;
        public int count;
        public int BoldCount;
        public int ItalicCount;
        public int h1Count;
        public int h2Count;
        public int h3Count;
        public int h4Count;
        public int h5Count;
        public int h6Count;
        public int TitleCount;
        public String imageSrc;
        public String bodyPrev;
        public ArrayList<Integer> index = new ArrayList<>();
    }

    public static String getImageFromDoc(Document doc, String word, URL url)
    {
        Elements elements = doc.select("img");
        String baseURL = "https://" + url.getHost();

        for (Element e: elements)
        {
            String alt = e.attr("alt");
            String title = e.attr("title");
            String absoluteURL = e.attr("src");

            if (absoluteURL.equals("") || (!absoluteURL.startsWith("http") && !absoluteURL.startsWith("/")) ) // if no absolute url, then might be an inside reference
            {
                absoluteURL = e.attr("data-src");
                if (absoluteURL.equals("") || (!absoluteURL.startsWith("http") && !absoluteURL.startsWith("/")))
                {
                    continue;
                }
                else
                {
                    if (!absoluteURL.startsWith("http"))
                    {
                        absoluteURL = baseURL + absoluteURL;
                    }
                }
            }
            else
            {
                if (!absoluteURL.startsWith("http")) // starts with '/'
                {
                    absoluteURL = baseURL + absoluteURL;
                }
            }

            if (!stringHasSequence(alt, word) && !stringHasSequence(title, word) && !stringHasSequence(absoluteURL, word))
            {
                continue;
            }

            return absoluteURL;
        }
        return "";
    }

    public static boolean stringHasSequence(String mainString, String sequence)
    {
        if (mainString == null || mainString.equals("") || (!mainString.toLowerCase().contains(sequence.toLowerCase())))
        {
            return false;
        }
        return true;
    }

    public static void dbInsertionCompletion(String urlAsString, Map<String, Word> wordsMap, DB_Connection db,String date,String location,int wordsCount)
    {
        List<Word> wordArr = new ArrayList<>(wordsMap.values());
        db.insertIntoIndexerTableBatch(urlAsString, wordArr);
        db.insertIntoIndexedURLs(urlAsString,date,location,wordsCount);
    }


    public static String getBodyPreviewFromElements(Elements elements, String word)
    {
        String returned = "";
        for (Element e: elements)
        {
            String lineIn = e.text().toString();
            if (!lineIn.toLowerCase().contains(word.toLowerCase()))
                continue;
            returned = returned + getBodyPreview(lineIn, word);
        }
        return returned;
    }

    public static String getBodyPreviewFromDoc(Document doc, String word, String rescueLine)
    {
        Elements h1Elements = doc.select("h1");
        Elements h2Elements = doc.select("h2");
        Elements h3Elements = doc.select("h3");
        Elements h4Elements = doc.select("h4");
        Elements h5Elements = doc.select("h5");
        Elements h6Elements = doc.select("h6");
        Elements pElements = doc.select("p");

        String returned = "";

        returned = getBodyPreviewFromElements(h1Elements, word);
        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(h2Elements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(h3Elements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(h4Elements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(h5Elements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(h6Elements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreviewFromElements(pElements, word);

        if (returned.length() < PREVIEW_LENGTH)
            returned = returned + getBodyPreview(rescueLine, word);

        return returned;

    }

    public static String getBodyPreview(String lineIn, String word)
    {
        String line1 = lineIn.replaceAll("\\.com", "");
        String line = line1.replaceAll("\\.net", "");
        String[] lineArr = line.split("\\. ");
        String returned = "";

        int j = 0;
        for (String s: lineArr)
        {
            if (s.toLowerCase().contains(word.toLowerCase()))
            {
                returned = s;
                break;
            }
            j++;
        }

        if (returned.equals(""))
        {
            return "";
        }

        int jReversed = j-1;
        j += 1;
        while (returned.length() < PREVIEW_LENGTH && lineArr.length > j)
        {
            returned = returned + ". " + lineArr[j];
            j++;
        }

        if (!returned.endsWith(".") && !returned.endsWith(". "))
        {
            returned = returned + ". ";
        }

        returned = returned.replaceAll("[*{}#]", "");
        returned = returned.replaceAll(" {2,}", ". ");

        return returned;
    }

    //DATE AND LOCATION HELPER FUNCTIONS////////////////////////////////////////
    public static String extractDate(String stringWitDate){
        Pattern pattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
        Matcher matcher = pattern.matcher(stringWitDate);
        if(matcher.find()){
            return (matcher.group(0));
        }
        return "0001-01-01";
    }

    public static String extractDateFromJSON(String JSONString){
        Pattern pattern = Pattern.compile("\"datePublished\".*[\\s\t\n,]");
        Matcher matcher = pattern.matcher(JSONString);
        if(matcher.find()){
            return (extractDate(matcher.group(0)));
        }

        return "0001-01-01";
    }

    public static String extractURLExtension(String domain){
        Pattern pattern = Pattern.compile("\\.co.*");
        Matcher matcher = pattern.matcher(domain);
        if(matcher.find()){
            String extension = (matcher.group(0));
            return (extension);
        }
        return(".com");
    }

    ////////////////////////////////////////////////////////////////////////////


    //DATE AND LOCATION MAIN FUNCTIONS/////////////////////////////////////////
    public static String getPageLocation(String url) throws MalformedURLException {
        URL myUrl = new URL(url);
        String domain = myUrl.getHost();
        String extension = extractURLExtension(domain);
        if(domain.endsWith(".uk") ){
            return "United Kingdom";
        }
        else if(domain.endsWith(".eg")){
            return "Egypt";
        }
        else if(domain.endsWith(".sa")){
            return "Saudi Arabia";
        }
        else if(domain.endsWith(".es")){
            return "Spain";
        }
        else if(domain.endsWith(".us")){
            return "United States";
        }
        else if(domain.endsWith(".fr")){
            return "France";
        }
        else if(domain.endsWith(".it")){
            return "Italy";
        }
        else if(domain.endsWith(".ae")){
            return "United Arab Emirates";
        }
        else if(domain.endsWith(".de")){
            return "Germany";
        }
        else if(domain.endsWith(".pt")){
            return "Portugal";
        }
        else if(domain.endsWith(".ru")){
            return "Russia";
        }
        else if(domain.endsWith(".in")){
            return "India";
        }
        else{
            return "Global";
        }
    }

    public static String getPubDate(Document doc) {

        Elements meta = doc.select("meta[itemprop]");

        for(Element element : meta){
            String itemProp = element.attr("itemprop");
            if(itemProp.equals("datePublished")){
                String date = extractDate(element.attr("content"));
                return date;
            }
        }

        meta = doc.select("time[itemprop]");

        for(Element element : meta){
            String itemProp = element.attr("itemprop");
            if(itemProp.equals("datePublished")){
                String date = extractDate(element.attr("datetime"));
                return date;
            }
        }

        meta = doc.select("script[type]");

        for(Element element : meta){
            String itemProp = element.attr("type");
            if(itemProp.equals("application/ld+json")){

                String date = extractDateFromJSON(element.html());
                return date;
            }
        }


        meta = doc.select("time[pubdate]");

        for(Element element : meta){
            String itemProp = element.attr("pubdate");
            if(itemProp.equals("pubdate")){
                String date = extractDate(element.attr("datetime"));
                return date;
            }
        }


        return "0001-01-01";
    }
}