package indexerPackage;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.net.URL;
import javax.print.DocFlavor;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Ranker {
    public static final float dampingFactor = (float)0.8;
    public static final float locationMultiplier = (float) 2;
    public static final float dateMultiplier = (float) 1;
    public static final float relevanceHelperFactor = (float) 200;
    public static final float relevanceComparator = (float)0.0005;
    public static final float Count = (float)0.1;
    public static final float Title = 10;
    public static final float H1 = 8;
    public static final float H2 = 7;
    public static final float H3 = 6;
    public static final float H4 = 5;
    public static final float H5 = 4;
    public static final float H6 = 3;
    public static final float Bold = 5;
    public static final float Italic = 5;



    public static void sortRank(ArrayList<URLRank> arr)
    {
        int n = arr.size();

        for (int i = 0; i < n-1; i++)
        {
            int min_idx = i;
            for (int j = i+1; j < n; j++)
                if (arr.get(j).rank < arr.get(min_idx).rank)
                    min_idx = j;

            Collections.swap(arr,min_idx,i);
        }
    }


    public static void sortPopularity(ArrayList<URLPopularity> arr)
    {
        int n = arr.size();

        for (int i = 0; i < n-1; i++)
        {
            int min_idx = i;
            for (int j = i+1; j < n; j++)
                if (arr.get(j).Popularity < arr.get(min_idx).Popularity)
                    min_idx = j;

            Collections.swap(arr,min_idx,i);
        }
    }

    public static void print(Object obj)
    {
        System.out.println(obj);
    }

    public static float CalculateRank(RowClass row, int totalNumberOfDocs, Map<String, DB_Connection.IndexedURLClass> indexedURLs, int numberOfDocsContainingWord)
    {
        //calculate tf
        int wordCountInRow = row.Count;
        int totalWordCount;

        try {
            totalWordCount = indexedURLs.get(row.URL).wordsCount;
        }
        catch (Exception e)
        {
            return (float) 0.0;
        }

        float tf = (float)wordCountInRow / (float)totalWordCount;

        float idf = (float) (Math.log((double)(totalNumberOfDocs)/(double)(numberOfDocsContainingWord)));

        float relevance = tf*idf;

        float relevanceHelper = (row.Count*Count)+(row.TitleCount*Title)+(row.H1Count*H1)+(row.H2Count*H2)+(row.H3Count*H3)+(row.H4Count*H4)+(row.H5Count*H5)+(row.H6Count*H6)+(row.BoldCount*Bold)+(row.ItalicCount*Italic);

        relevance = relevance * (1 + (relevanceHelper / relevanceHelperFactor));

        return relevance;

    }

    public static Map<String, Integer> getNumberOfURLsContainingWord(DB_Connection db, String[] arr)
    {
        Map<String, Integer> returned = new HashMap<>();
        for (String s: arr)
        {
            try {
                int numberOfURLs = db.getNumberOfURLsContainingWord(s);
                returned.put(s, numberOfURLs);
            } catch (SQLException e) {
                System.out.println(e.getLocalizedMessage());
            }
        }
        return returned;
    }

    public static Map<String, Integer> getLocalNumberOfURLsContainingWord(String[] arr, Map<String, List<RowClass>> map)
    {
        Map<String, Integer> returned = new HashMap<>();
        for (String s: arr)
        {
            int numberOfURLs = map.get(s).size();
            returned.put(s, numberOfURLs);
        }
        return returned;
    }

    //MODIFIED THIS FUNCTION TO INCREASE RANKS BASED ON LOCATION AND PAGE PUBLISHING DATE
    public static ArrayList<URLRank> Rank(DB_Connection db, Map<String, List<RowClass>> Rows,int searchQueryLength,boolean phraseSearching,Map<String,Integer> phraseURLS,String userLocation, String[] preProcessedSearch ) throws SQLException {
        Map<String,URLRank> URLRankMap = new HashMap<String, URLRank>();
        int iter = 0;
        System.out.println("Rows size = "+Rows.size());
        int totalNumberOfDocs = db.getNumberOfCrawledURLs();
        Map<String, DB_Connection.IndexedURLClass> indexedURLs = db.getAllIndexedURLs();
        Map<String, Integer> numberOfURLsContainingWordMap = getLocalNumberOfURLsContainingWord(preProcessedSearch, Rows);


        //loop over all the rows

        //loop over each list
        for (List<RowClass> list: Rows.values())
        {
            //loop over each row within el list
            for(RowClass r: list)
            {
                iter++;
                System.out.println("iteration num = "+iter);
                float rowRank = CalculateRank(r, totalNumberOfDocs, indexedURLs, numberOfURLsContainingWordMap.get(r.Word));
                if (rowRank == 0.0)
                {
                    Rows.remove(r.URL);
                    continue;
                }
                URLRank obj = URLRankMap.get(r.URL);
                if(obj == null){
                    obj = new URLRank();
                    obj.URL = r.URL;
                    obj.imageSrc = r.imageSrc;
                    obj.rank = rowRank;
                    obj.occurences = 1;
                    URLRankMap.put(r.URL,obj);
                }
                else{
                    obj.rank = obj.rank + rowRank;
                    obj.occurences++;
                }
            }

        }

        ArrayList<URLRank> arr = new ArrayList<URLRank>();

        //MODIFIED THIS FOR LOOP TO INCREASE RELEVANCE IN CASE OF USER LOCATION SIMILAR TO URL LOCATION

        for (Map.Entry<String, URLRank> entry:URLRankMap.entrySet()){
            URLRank obj = URLRankMap.get(entry.getValue().URL);
            String date = indexedURLs.get(obj.URL).date;
            String location = indexedURLs.get(obj.URL).location;

            if(userLocation.equals(location)){
                obj.rank = obj.rank * locationMultiplier;
            }

            if(!date.equals("0001-01-01")){
                Date currentDate = new Date();
                int daysFromPublish = 1;
                try{
                    daysFromPublish = getDaysBetweenDates(currentDate,date);
                }
                catch(ParseException e){
                    System.out.println(e.getMessage());
                }
                obj.rank = obj.rank * (1 + (1/(float)daysFromPublish));
            }

            arr.add(entry.getValue());
        }

        sortRank(arr);

        if(arr.size()<=1){return arr;}

        Map<String, Float> popularitiesMap = db.getAllPopularitiesList();
        for(int i=1; i<arr.size(); i++)
        {
            URLRank highRelevance = arr.get(i);
            URLRank lowRelevance = arr.get(i-1);
            int highRelevanceLocation = i;
            int lowRelevanceLocation = i-1;
            if(highRelevance.rank - lowRelevance.rank < relevanceComparator){

                print(highRelevance.URL);

                float highRelevancePopularity = (popularitiesMap.get(highRelevance.URL));
                float lowRelevancePopularity = (popularitiesMap.get(lowRelevance.URL));

                while(highRelevancePopularity < lowRelevancePopularity){

                    Collections.swap(arr,lowRelevanceLocation,highRelevanceLocation);
                    lowRelevanceLocation = lowRelevanceLocation-1;
                    highRelevanceLocation = highRelevanceLocation-1;
                    if(lowRelevanceLocation<0 || arr.get(highRelevanceLocation).rank - arr.get(lowRelevanceLocation).rank > relevanceComparator){
                        break;
                    }
                    else{
                        highRelevancePopularity = popularitiesMap.get(arr.get(highRelevanceLocation).URL);
                        lowRelevancePopularity = popularitiesMap.get(arr.get(lowRelevanceLocation).URL);
                    }
                }
            }
            else{
                continue;
            }

        }

        db.closeConnection();

        if (phraseSearching){

            ArrayList<URLRank> finalArr = new ArrayList<URLRank>();


            long tPhrase1 = System.currentTimeMillis();
            int arraySize = arr.size();

            for(int i=0;i<arraySize;i++){

                String currentURL = arr.get(i).URL;

                if(phraseURLS.get(currentURL) != null)
                {
                    URLRank obj = arr.get(i);
                    obj.rank = obj.rank * (float)phraseURLS.get(obj.URL).intValue();
                    finalArr.add(obj);
                }
            }

            sortRank(finalArr);

            long tPhrase2 = System.currentTimeMillis();
            float totalPhraseTime = ((float) (tPhrase2 - tPhrase1)) / 1000;

            System.out.println("Phrase Searching overhead 2 = " + totalPhraseTime + " s");

            return  finalArr;

        }

        return arr;
    }

    public static Map<String,URLPopularity> PopularityPreProcessing(DB_Connection db) {
        long t1 = System.currentTimeMillis();

        Map<String,URLPopularity> urls = new HashMap<String,URLPopularity>();
        int urlsWithIngoing = 0;
        //get all crawled URLS
        ArrayList<DB_Connection.WebLink> weblinks = db.getLinks();
        ArrayList<String> links = new ArrayList<String>();
        for(int i=0;i<weblinks.size();i++){
            links.add(weblinks.get(i).url);
        }
        db.closeConnection();

        for(int i=0;i<links.size();i++){
            String url = links.get(i);
            URLPopularity obj = new URLPopularity();
            obj.URL = url;
            urls.put(url,obj);
        }


        //This for loop will Jsoup into each URL, and check each link inside of it, if this link belongs to our crawler table, then we will increase the number of outgoing urls for the outer url
        //And we will also set the outer url as an ingoing url to the found link
        int iteration = 1;
        for(int i=0;i< links.size();i++){

            System.out.println("Iteration: "+iteration);
            iteration++;
            String url = links.get(i);
            System.out.println("URL = " + url);

            Document doc;
            try{
                doc = Jsoup.connect(url).timeout(0).get();
            }
            catch (IOException e){
                System.out.println(e.getMessage());
                urls.get(url).countOfOutgoing=0;
                continue;
            }

            URL myurl;
            try{
                myurl = new URL(url);
            }
            catch (IOException e){
                System.out.println(e.getMessage());
                urls.get(url).countOfOutgoing=0;
                continue;
            }

            Elements link = doc.select("a");
            int outgoingCount = 0;
            for(Element element : link){
                String href = element.attr("href");
                if(!href.equals("") && !href.equals("#")){
                    String finalHref;
                    if(href.startsWith("/")){
                        finalHref = myurl.getProtocol()+"://"+myurl.getHost()+href;
                    }
                    else{
                        finalHref=href;
                    }

                    URLPopularity obj = urls.get(finalHref);

                    if(obj != null ){
                        if (!url.equals(finalHref)) {
                            if(obj.ingoingUrls.get(url) == null){
                                obj.ingoingUrls.put(url,url);
                                outgoingCount++;
                                if(obj.ingoingUrls.size() == 1){
                                    urlsWithIngoing++;
                                }
                            }
                        }
                    }
                }
            }
            urls.get(url).countOfOutgoing = outgoingCount;
        }


        //calculate the initial popularity
        for (Map.Entry<String, URLPopularity> entry:urls.entrySet()){
            String url = entry.getValue().URL;
            URLPopularity obj = urls.get(url);
            if(obj.ingoingUrls.size() == 0)
            {
                obj.Popularity = 0;
            }
            else{
                obj.Popularity=1;
            }
        }

        long t2 = System.currentTimeMillis();

        float totalTime = ((float) (t2-t1)) / 1000;
        System.out.println("Total time taken to preprocess all crawled urls = "+ totalTime);
        return urls;
    }

    public static void PopularityCalculator(Map<String,URLPopularity> urlMap, DB_Connection db){
        long t1 = System.currentTimeMillis();
        int iteration = 0;

        long calct1 = System.currentTimeMillis();
        while(true){
            boolean breakFlag = true;
            iteration++;
            for (Map.Entry<String, URLPopularity> entry:urlMap.entrySet()){
                boolean popularityChange = false;
                URLPopularity obj = urlMap.get(entry.getValue().URL);
                double currentPopularity = 0;
                for (Map.Entry<String, String> entryB:entry.getValue().ingoingUrls.entrySet()){

                    if(urlMap.get(entryB.getValue()).countOfOutgoing == 0){
                        continue;
                    }
                    popularityChange = true;
                    double tempPopularity = urlMap.get(entryB.getValue()).Popularity;
                    double tempOutgoingCount = urlMap.get(entryB.getValue()).countOfOutgoing;
                    currentPopularity = currentPopularity + (tempPopularity / tempOutgoingCount);
                }
                double oldPopularity = obj.Popularity;
                if(popularityChange == true){
                    currentPopularity = currentPopularity*dampingFactor;
                    currentPopularity=currentPopularity+(1-dampingFactor);
                    obj.tempPopularity = currentPopularity;
                    if((obj.tempPopularity - oldPopularity) > 0.01 || (oldPopularity - obj.tempPopularity) > 0.01){
                        breakFlag = false;
                    }

                }
                else{
                    obj.tempPopularity=0;
                }

            }

            if(breakFlag == true || iteration == 500){
                break;
            }
            //or count == 1000

            double sum = 0;
            for (Map.Entry<String, URLPopularity> entry:urlMap.entrySet()){
                URLPopularity obj = urlMap.get(entry.getValue().URL);
                obj.Popularity = obj.tempPopularity;
                sum = sum +obj.Popularity;
            }
            System.out.println("Summation after iteration is :"+sum);

        }
        long calct2 = System.currentTimeMillis();
        long totalCalctime = calct2 - calct1;
        System.out.println("Time taken to calculate Popularity = "+ totalCalctime);

        //Insert into arraylist
        ArrayList<URLPopularity> urlArrayList = new ArrayList<URLPopularity>();
        for (Map.Entry<String, URLPopularity> entry:urlMap.entrySet()){
            urlArrayList.add(entry.getValue());
        }
        //sort according to Popularity
        sortPopularity(urlArrayList);

        long insertt1 = System.currentTimeMillis();
        for(int i=urlArrayList.size()-1;i>=0;i--){
            db.insertIntoPopularity(urlArrayList.get(i).URL,(float)urlArrayList.get(i).Popularity);
        }
        long insertt2 = System.currentTimeMillis();

        long totalInsertTime = insertt2 - insertt1;

        System.out.println("Total time taken to insert popularities = "+totalInsertTime);

        long t2 = System.currentTimeMillis();
        long total = t2 -t1;
        System.out.println("Total time taken to calculate and insert popularity = "+ total);

    }

    public void popularity(DB_Connection db){
        Map<String,URLPopularity> myMap = new HashMap<String,URLPopularity>();

        myMap = PopularityPreProcessing(db);

        PopularityCalculator(myMap, db);
    }


    //////////////////NEW FUNCTIONS FOR THE DATE AND LOCATION PART///////////////

    public static Date stringToDate(String date) throws ParseException {

        Date d = new SimpleDateFormat("yyyy-MM-dd").parse(date);
        return d;
    }

    public static int getDaysBetweenDates(Date moreRecentDate,String lessRecentDate) throws ParseException {

        Date d2 = stringToDate(lessRecentDate);
        long diff = moreRecentDate.getTime() - d2.getTime();
        int diffDays = (int)(diff/(24*60*60*1000));
        diffDays++;
        return diffDays;

    }
/////////////////////////////////////////////////////////////////////////////
}

