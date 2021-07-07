import indexerPackage.DB_Connection;
import indexerPackage.Indexer;

public class IndexerMain {

    public static void main(String[] args)
    {
        String tableName = "crawlerx";
        new Indexer(new DB_Connection(tableName, "testa", "testa"));
    }

}
