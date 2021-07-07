package indexerPackage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class URLPopularity {
    String URL;
    int countOfOutgoing;
    Map<String,String> ingoingUrls = new HashMap<String,String>();
    double Popularity;
    double tempPopularity;


    public void print(){
        System.out.println("URL = "+URL+"  Count of outgoing = "+countOfOutgoing+"  ingoing links = ");

        for (Map.Entry<String,String> entry:ingoingUrls.entrySet()){
            System.out.println(entry.getValue());
        }


        System.out.println("  Popularity = "+Popularity);
    }
}