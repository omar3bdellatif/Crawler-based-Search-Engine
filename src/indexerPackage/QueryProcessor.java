package indexerPackage;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.tartarus.snowball.ext.englishStemmer;

import javax.print.Doc;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryProcessor {

    public static DB_Connection db;

    public static void setStopWords(Map<String, String> stopwordsMap) {
        String stopWords[] = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};
        for (int i = 0; i < stopWords.length; i++) {
            stopwordsMap.put(stopWords[i], stopWords[i]);
        }
    }

    public static String StemWords(String word) {
        englishStemmer stemmer = new englishStemmer();
        stemmer.setCurrent(word);
        stemmer.stem();
        return stemmer.getCurrent();
    }


    public static String[] PreProcessing(String searchQuery) {
        String preProcessed = "";
        Map<String,String> stopwordsMap = new HashMap<String,String>();
        String skippingSpecialCharacters= searchQuery.replaceAll("[0-9–.©+=:_#$%^&*~'’,()\\?\\/*@<>{}|\"\\\\!;-]+"," ");
        setStopWords(stopwordsMap);

        String[] arr = skippingSpecialCharacters.split("[\\s\t\n]+");
        for (int j = 0; j < arr.length; j++) {
            if (arr[j].matches("[ \t\n]*")) {
                continue;
            }
            String caseInsensitive = arr[j].toLowerCase();
            String stemmed = StemWords(caseInsensitive);

            if (stopwordsMap.get(stemmed) != null) {
                continue;
            }
            preProcessed = preProcessed + stemmed +" ";

        }
        String[] preProcessedArray = preProcessed.split(" ");
        return preProcessedArray;
    }

    public static Map<String, List<RowClass>> LookUp(String[] preProcessed) throws SQLException {
        db = new DB_Connection("crawlerx");
        Map<String, List<RowClass>> Row;
        Row = db.LookUp(preProcessed);
        return Row;
    }

    public static Map<String, List<RowClass>> getRows(String[] result){
        Map<String, List<RowClass>> Rows;
        for(int i=0;i<result.length;i++){
            System.out.println(result[i]);
        }

        try{
            Rows = LookUp(result);
            return Rows;
        }
        catch (SQLException e){
            System.out.println(e.getMessage());
        }

        return new HashMap<>();
    }


    /////////////PhraseSearching///////////////////////

    public static Map<String,Integer> phraseSearchFilter(String rawSearchQuery, String[] pre, Map<String, List<RowClass>> Rows){

        ArrayList<String> candidatePhraseURLs = getCandidatePhraseURLS(rawSearchQuery, pre, Rows);
        Map<String,Integer> validPhraseURLs = new HashMap<String,Integer>();

        for(int i=0;i<candidatePhraseURLs.size();i++){

            String currentURL = candidatePhraseURLs.get(i);
            int count = 0;
            try{
                count = validCandidatePhraseURL(currentURL,rawSearchQuery);
            }
            catch (IOException e){
                System.out.println(e.getMessage());
            }

            if(count != 0){
                Integer countInteger = count;
                validPhraseURLs.put(currentURL,countInteger);
            }

        }

        return validPhraseURLs;


    }


    //function to get candidate urls

    public static ArrayList<String> getCandidatePhraseURLS(String rawSearchQuery, String[] pre, Map<String, List<RowClass>> Rows){

        ArrayList<String> candiatePhraseURLS = new ArrayList<String>();
        int searchQueryLength = getNumberOfWords(rawSearchQuery);
        Map<String,Integer> URLtoOccurences = new HashMap<String,Integer>();

        //loop over each list
        for (List<RowClass> list: Rows.values())
        {
            //loop over each row
            for (RowClass r: list)
            {
                String currentURL = r.URL;
                Integer occurences = URLtoOccurences.get(currentURL);

                if(occurences == null){
                    occurences = 1;
                    URLtoOccurences.put(currentURL,occurences);
                }
                else{
                    occurences = occurences + 1;
                    URLtoOccurences.put(currentURL,occurences);
                }

            }
        }

        for (Map.Entry<String, Integer> entry:URLtoOccurences.entrySet()){
            String currentURL = entry.getKey();
            Integer occurences = entry.getValue();

            if(occurences.intValue() == searchQueryLength){
                candiatePhraseURLS.add(currentURL);
            }
        }

        return candiatePhraseURLS;


    }


    //function tehaded el candidate url da valid walla la2


    public static int validCandidatePhraseURL(String URL,String rawSearchQuery) throws IOException {


        Document doc = Jsoup.connect(URL).get();

        String documentText = doc.body().text();
        String lowerCaseDocText=documentText.toLowerCase();
        String lowerCaseRawSearchQuery=rawSearchQuery.toLowerCase();


        int fromIndex = 0;
        int count = 0;
        boolean flag = false;

        while(true){
            int exists = lowerCaseDocText.indexOf(lowerCaseRawSearchQuery,fromIndex);
            if(exists != -1){
                count = count + 1;
                fromIndex = exists+1;
            }
            else{
                break;
            }
        }

        return count;



    }

    public static int getNumberOfWords(String string){
        String[] arr = PreProcessing(string);
        return arr.length;
    }

    public static boolean isPhraseSearching(String rawSearchQuery){
        if(rawSearchQuery.startsWith("\"") && rawSearchQuery.endsWith("\"")){
            return true;
        }
        else{
            return false;
        }
    }

    public static String removeQuotations(String rawSearchQuery)
    {
        return rawSearchQuery.substring(1,rawSearchQuery.length()-1);
    }
    ///////////////////////////////////////////////////

    public static void main(String[] args) throws SQLException
    {
        int searchQueryLength;
        boolean isPhraseSearching;
        Map<String, List<RowClass>> Rows;
        ArrayList<URLRank> rankedURLS;
        Map<String,Integer> phraseURLsMap = new HashMap<String,Integer>();

        String rawSearchQuery = "tennis";

        long t1 = System.currentTimeMillis();

        String[] pre = PreProcessing(rawSearchQuery);

        long trows1 = System.currentTimeMillis();
        Rows = getRows(pre);
        long trows2 = System.currentTimeMillis();
        float timeTakenRows = (float)(trows2 - trows1) / 1000;
        Ranker.print("get Rows Time = " + timeTakenRows);

        isPhraseSearching = isPhraseSearching(rawSearchQuery);

        if(isPhraseSearching)
        {
            long tPhrase1 = System.currentTimeMillis();

            rawSearchQuery = removeQuotations(rawSearchQuery);
            phraseURLsMap = phraseSearchFilter(rawSearchQuery, pre, Rows);

            long tPhrase2 = System.currentTimeMillis();
            float phraseOverHeader = (float)(tPhrase2  - tPhrase1) / 1000;
            System.out.println("Phrase Searching overhead 1 = "+phraseOverHeader + " s");
        }

        searchQueryLength = getNumberOfWords(rawSearchQuery);


        Ranker r = new Ranker();
        long tRank1 = System.currentTimeMillis();
        rankedURLS=r.Rank(db, Rows, searchQueryLength, isPhraseSearching,phraseURLsMap,"United Kingdom", pre);
        long tRank2 = System.currentTimeMillis();

        float totalRankingTime = (float)(tRank2 - tRank1) / 1000;
        System.out.println("Ranking time = "+totalRankingTime);

        long t2 = System.currentTimeMillis();
        float timeTaken = (float)(t2 - t1) / 1000;



        for(int i=0;i<rankedURLS.size();i++)
        {
            System.out.println("URL = "+rankedURLS.get(i).URL + " Rank = "+rankedURLS.get(i).rank);
        }

        System.out.println("Total Time taken = " + timeTaken + " s");
    }
}
