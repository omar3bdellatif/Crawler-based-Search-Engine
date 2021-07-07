import indexerPackage.DB_Connection;
import indexerPackage.Ranker;
import org.jsoup.nodes.Document;

import java.util.Map;

public class RankerMain {

    public static void main(String[] args)
    {
        String tableName = "crawlerx";
        DB_Connection db = new DB_Connection(tableName);
        Ranker ranker = new Ranker();
        long t1 = System.currentTimeMillis();
        ranker.popularity(db);
        long t2 = System.currentTimeMillis();
        System.out.println("Ranker time = " + (t2-t1) + " ms");
    }

}
