package indexerPackage;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.*;
import java.util.*;

public class DB_Connection {

    public Connection conn;

    public DB_Connection(String dbname, String username, String pass) {
        this.conn = null;
        startConnection(dbname, username, pass);
    }

    public DB_Connection(String dbname) {
        this.conn = null;
        startConnection(dbname, "root", "");
    }

    //I SET THIS FUNCTION TO PUBLIC, I DON'T SEE THE POINT OF MAKING IT A PRIVATE FUNCTION
    public void closeConnection() {
        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void startConnection(String dbname, String username, String pass) {
        try {
            //MODIFIED THE FOLLOWING LINE (ADDED EL cj PART)
            //I ALSO HAD TO RUN THE FOLLOWING QUERY (  SET GLOBAL time_zone = '+2:00') BECAUSE OF SOME TIME ZONE ISSUE
            Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbname,
                    username, pass);
            if (!this.conn.isClosed())
                System.out.println("Successfully connected to MySQL server...");
        } catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
        }
    }

    public int insertValueToWebLinks(String url, String title, Boolean crawled) {

        try {
            PreparedStatement ps = this.conn.prepareStatement("insert into weblinks values(?,?,?)");
            ps.setString(1, url);
            ps.setString(2, title);
            ps.setBoolean(3, crawled);

            return ps.executeUpdate();
        } catch (Exception e) {

        }
        return 0;
    }


    public int updateCrawled(String url, String title) {
        try {
            PreparedStatement ps = this.conn.prepareStatement("UPDATE `weblinks` SET `Title`=?,`crawled`=? WHERE urlName=?");
            ps.setString(3, url);
            ps.setString(1, title);
            ps.setBoolean(2, true);
            return ps.executeUpdate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }

    public void insertIntoIndexedURLs(String url,String date,String location,int wordsCount)
    {
        try
        {
            PreparedStatement ps = this.conn.prepareStatement("INSERT INTO `indexedurls`(`links`, `pubdate`, `location`, `wordsCount`) VALUES (?,?,?,?)");
            ps.setString(1, url);
            ps.setDate(2,java.sql.Date.valueOf(date));
            ps.setString(3,location);
            ps.setInt(4,wordsCount);
            ps.executeUpdate();

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }


    public int insertIntoPositions(String url, String word, int index) {
        try {
            PreparedStatement ps = this.conn.prepareStatement("INSERT IGNORE INTO `positions`(`URLs`, `Words`, `Index`) VALUES (?,?,?)");
            ps.setString(1, url);
            ps.setString(2, word);
            ps.setInt(3, index);
            return ps.executeUpdate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }

    public void insertIntoPopularity(String url, float PopularityOrder) {
        try {
            PreparedStatement ps = this.conn.prepareStatement("INSERT INTO `popularity`(`URLs`, `PopularityOrder`) VALUES (?,?)");
            ps.setString(1, url);
            ps.setFloat(2, PopularityOrder);
            ps.executeUpdate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertIntoPopularityBatch(List<URLPopularity> urlPopularitiesList) {
        try(
                PreparedStatement ps = this.conn.prepareStatement("INSERT INTO `popularity`(`URLs`, `PopularityOrder`) VALUES (?,?)")
        )
        {
            int i = 0;
            for (URLPopularity urlPopularity: urlPopularitiesList)
            {
                ps.setString(1, urlPopularity.URL);
                ps.setFloat(2, (float) urlPopularity.Popularity);
                ps.addBatch();
                i += 1;
                if (i % 500 == 0 || i == urlPopularitiesList.size())
                {
                    ps.executeBatch();
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public void insertIntoIndexerTable(String url, Indexer.Word wordObj) {
        try {
            PreparedStatement ps = this.conn.prepareStatement("INSERT IGNORE INTO `indexer`(`URLs`, `Words`, `Count`, `H1Count`, `H2Count`, `H3Count`, `H4Count`, `H5Count`, `H6Count`, `BoldCount`, `ItalicCount`, `TitleCount`, `ImageSrc`, `BodyPrev`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps.setString(1, url);
            ps.setString(2, wordObj.word);
            ps.setInt(3, wordObj.count);
            ps.setInt(4, wordObj.h1Count);
            ps.setInt(5, wordObj.h2Count);
            ps.setInt(6, wordObj.h3Count);
            ps.setInt(7, wordObj.h4Count);
            ps.setInt(8, wordObj.h5Count);
            ps.setInt(9, wordObj.h6Count);
            ps.setInt(10, wordObj.BoldCount);
            ps.setInt(11, wordObj.ItalicCount);
            ps.setInt(12, wordObj.TitleCount);
            ps.setString(13, wordObj.imageSrc);
            ps.setString(14, wordObj.bodyPrev);

//            new psExecutor(ps).start();
            ps.executeUpdate();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }


    public void insertIntoIndexerTableBatch(String url, List<Indexer.Word> wordList) {
        try (
                PreparedStatement ps = this.conn.prepareStatement("INSERT IGNORE INTO `indexer`(`URLs`, `Words`, `Count`, `H1Count`, `H2Count`, `H3Count`, `H4Count`, `H5Count`, `H6Count`, `BoldCount`, `ItalicCount`, `TitleCount`, `ImageSrc`, `BodyPrev`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ) {
            int i = 0;

            for (Indexer.Word wordObj: wordList)
            {
                ps.setString(1, url);
                ps.setString(2, wordObj.word);
                ps.setInt(3, wordObj.count);
                ps.setInt(4, wordObj.h1Count);
                ps.setInt(5, wordObj.h2Count);
                ps.setInt(6, wordObj.h3Count);
                ps.setInt(7, wordObj.h4Count);
                ps.setInt(8, wordObj.h5Count);
                ps.setInt(9, wordObj.h6Count);
                ps.setInt(10, wordObj.BoldCount);
                ps.setInt(11, wordObj.ItalicCount);
                ps.setInt(12, wordObj.TitleCount);
                ps.setString(13, wordObj.imageSrc);
                ps.setString(14, wordObj.bodyPrev);
                ps.addBatch();
                i += 1;
                if (i % 500 == 0 || i == wordList.size())
                {
                    ps.executeBatch();
                }
            }
        } catch (Exception e)
        {
            System.out.println("DB Indexer Insertion Error: " + e.getMessage());
        }


    }


    public int insertValueToWebLinks(URL url) {
        return this.insertValueToWebLinks(url.toString(), "", false);
    }


    public int customInsert(String tableName, String values) {
        PreparedStatement ps = null;
        try {
            String query = "insert ignore into " + tableName + " values (?)";
            ps = this.conn.prepareStatement(query);
            ps.setString(1, values);
            return ps.executeUpdate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }

    }

    public static class WebLink {
        String url;
        String title;
        boolean crawled;
    }

    public ArrayList<WebLink> getLinks() {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM weblinks WHERE `crawled`=1");
            ResultSet rs = stmt.executeQuery();
            ArrayList<WebLink> Links = new ArrayList<>();
            while (rs.next()) {
                WebLink temp = new WebLink();
                temp.url = rs.getString(1);
                temp.title = rs.getString(2);
                temp.crawled = rs.getBoolean(3);
                Links.add(temp);
            }
            return Links;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return new ArrayList<>();
        }
    }




    public Map<String, Document> getCrawledLinksAsMap()
    {
        Map<String, Document> returned = new HashMap<>();
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT `urlName`, `Document` FROM weblinks WHERE `crawled`=1");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String url = rs.getString(1);
                Document document = Jsoup.parse(rs.getString(2));
                returned.put(url, document);
            }
            return returned;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return returned;
        }
    }



    //For crawling after stoppage
    public Set<URL> getCrawledLinks() throws SQLException, MalformedURLException {
        PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM weblinks WHERE `crawled`=1");
        ResultSet rs = stmt.executeQuery();
        Set<URL> Links = new HashSet<>();

        while (rs.next()) {
            Links.add(new URL(rs.getString(1)));
        }
        return Links;
    }


    public Set<URL> getNonCrawledLinks() throws SQLException, MalformedURLException {
        PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM weblinks WHERE `crawled`=0");
        ResultSet rs = stmt.executeQuery();
        Set<URL> Links = new HashSet<>();

        while (rs.next()) {
            Links.add(new URL(rs.getString(1)));
        }
        return Links;
    }


    public Map<String, String> getIndexedURLs() throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM indexedurls");
        ResultSet rs = stmt.executeQuery();
        Map<String, String> indexedURLs = new HashMap<String, String>();
        while (rs.next()) {
            indexedURLs.put(rs.getString(1), rs.getString(1));
        }
        return indexedURLs;
    }

    public static class IndexedURLClass
    {
        String url;
        String location;
        String date;
        int wordsCount;
    }

    public Map<String, IndexedURLClass> getAllIndexedURLs() throws SQLException {
        PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM indexedurls");
        ResultSet rs = stmt.executeQuery();
        Map<String, IndexedURLClass> indexedURLs = new HashMap<>();
        while (rs.next()) {
            IndexedURLClass indexed = new IndexedURLClass();
            indexed.url = rs.getString(1);
            indexed.date = rs.getDate(2).toString();
            indexed.location = rs.getString(3);
            indexed.wordsCount = rs.getInt(4);
            indexedURLs.put(indexed.url, indexed);
        }
        return indexedURLs;
    }

    public ArrayList<String> getLinksByWord(String word) throws SQLException {
        String sql = "SELECT DISTINCT `URLs` FROM `indexer` WHERE Words = '" + word + "'";
        System.out.println(sql);
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        ArrayList<String> Links = new ArrayList<String>();
        while (rs.next()) {
            Links.add(rs.getString(1));
        }
        return Links;
    }

    public int getNumberOfURLsContainingWord(String word) throws SQLException {
        String sql = "SELECT COUNT(*) FROM `indexer` WHERE Words = '" + word + "'";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int URLsCount = rs.getInt(1);
        return URLsCount;
    }

    public Map<String, List<RowClass>> LookUp(String[] PreProcessed) throws SQLException {
        Map<String, List<RowClass>> rowsReturned = new HashMap<>();

        String sql = "SELECT * FROM `indexer` WHERE `Words`=";
        sql += "'" + PreProcessed[0] + "' ";
        rowsReturned.put(PreProcessed[0], new ArrayList<>());
        for (int i=1; i< PreProcessed.length; i++)
        {
            sql += "OR `Words`='" + PreProcessed[i] + "' ";
            rowsReturned.put(PreProcessed[i], new ArrayList<>());
        }
        sql.substring(0, sql.length()-1);

        System.out.println(sql);
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            RowClass row = new RowClass();
            row.URL = rs.getString(1);
            row.Word = rs.getString(2);
            row.Count = rs.getInt(3);
            row.H1Count = rs.getInt(4);
            row.H2Count = rs.getInt(5);
            row.H3Count = rs.getInt(6);
            row.H4Count = rs.getInt(7);
            row.H5Count = rs.getInt(8);
            row.H6Count = rs.getInt(9);
            row.BoldCount = rs.getInt(10);
            row.ItalicCount = rs.getInt(11);
            row.TitleCount = rs.getInt(12);
            row.imageSrc = rs.getString(13);
            row.bodyPrev = rs.getString(14);
            rowsReturned.get(row.Word).add(row);
        }

        return rowsReturned;
    }

    public ArrayList<String> getWordsByLink(String url) throws SQLException {
        String sql = "SELECT `Words` FROM `indexer` WHERE URLs = '" + url + "'";
        System.out.println(sql);
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        ArrayList<String> Links = new ArrayList<String>();
        while (rs.next()) {
            Links.add(rs.getString(1));
        }
        return Links;
    }

    public float getPopularityOrder(String url) throws SQLException {
        String sql = "SELECT `PopularityOrder` FROM `popularity` WHERE URLs = '" + url + "'";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        float popularityOrder = rs.getFloat(1);
        return popularityOrder;
    }

    public String getURLLocation(String url) throws SQLException {
        String sql = "SELECT `location` FROM `indexedurls` WHERE links = '"+ url + "'";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String location = rs.getString(1);
        return location;
    }

    public String getURLDate(String url) throws SQLException {
        String sql = "SELECT `pubdate` FROM `indexedurls` WHERE links = '"+ url + "'";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String date = rs.getDate(1).toString();
        return date;
    }

    public int getNumberOfWordsInURL(String url) throws SQLException {
        String sql = "SELECT wordsCount FROM `indexedurls` WHERE links = '"+url+"'";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int wordsCount = rs.getInt(1);
        return wordsCount;
    }

    public int getNumberOfCrawledURLs() throws SQLException {
        String sql = "SELECT Count(*) FROM `weblinks` WHERE `crawled`=1";
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int URLsCount = rs.getInt(1);
        return  URLsCount;
    }

    public List<String> getPopularitiesList()
    {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM `popularity`");
            ResultSet rs = stmt.executeQuery();
            ArrayList<String> links = new ArrayList<>();
            while (rs.next()) {
                links.add(rs.getString(1));
//                temp.url = rs.getString(1);
//                temp.title = rs.getString(2);
//                temp.crawled = rs.getBoolean(3);
//                Links.add(temp);
            }
            return links;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return new ArrayList<>();
        }
    }

    public void deleteFromPopularity(String url)
    {
        String sql = "DELETE FROM `popularity` WHERE `URLs` = '"+url+"'";
        try {
            Statement statement = this.conn.createStatement();
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getLocalizedMessage());
        }

    }

    public List<String> getURLsInIndexer()
    {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM `indexer` GROUP BY `indexer`.`URLs`");
            ResultSet rs = stmt.executeQuery();
            ArrayList<String> links = new ArrayList<>();
            while (rs.next()) {
                links.add(rs.getString(1));
            }
            return links;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return new ArrayList<>();
        }
    }

    public Map<String, String> getURLsInIndexedURLs()
    {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM `indexedurls` GROUP BY `indexedurls`.`links`");
            ResultSet rs = stmt.executeQuery();
            Map<String, String> links = new HashMap<>();
            while (rs.next()) {
                links.put(rs.getString(1), rs.getString(1));
            }
            return links;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return new HashMap<>();
        }
    }


    public void deleteFromIndexerWithURL(String url)
    {
        try
        {
            String sql = "DELETE FROM `indexer` WHERE `URLs`='" + url + "'";
            Statement statement = this.conn.createStatement();
            statement.executeUpdate(sql);

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }


    public Map<String, Float> getAllPopularitiesList()
    {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM `popularity`");
            ResultSet rs = stmt.executeQuery();
            Map<String, Float> links = new HashMap<>();
            while (rs.next()) {
                links.put(rs.getString(1), rs.getFloat(2));
            }
            return links;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return new HashMap<>();
        }
    }


    public static class queryExecutor extends Thread {
        String query;
        DB_Connection db;

        public queryExecutor(DB_Connection db, String query) {
            this.db = db;
            this.query = query;
        }

        @Override
        public void run() {
            try {
                PreparedStatement ps = this.db.conn.prepareStatement(query);
//                ps.setString(1, url);
                ps.executeUpdate();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return;
            }
        }
    }

    public static class psExecutor extends Thread {
        PreparedStatement ps;

        public psExecutor(PreparedStatement ps) {
            this.ps = ps;
        }

        @Override
        public void run() {
            try {
//                ps.setString(1, url);
                ps.executeUpdate();
            } catch (Exception e) {
//                System.out.println(e.getMessage());
                return;
            }
        }
    }


}