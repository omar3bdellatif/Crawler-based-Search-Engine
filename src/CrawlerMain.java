import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;

public class CrawlerMain {


    public static void print(Object obj)
    {
        System.out.println(obj);
    }

    public static void main(String[] args) throws IOException {
        Set<URL> initURLs = new HashSet<>();

        boolean reCrawl = true; // *TRUE* IF Database is empty OR need to delete the database and update all the links, *FALSE* Otherwise

        if (reCrawl)
        {
            initURLs.add(new URL("https://www.cookingchanneltv.com/"));
            initURLs.add(new URL("https://www.rottentomatoes.com/browse/in-theaters/"));
            initURLs.add(new URL("https://www.imdb.com/chart/moviemeter/"));
            initURLs.add(new URL("https://www.bbc.co.uk"));
            initURLs.add(new URL("https://cu.edu.eg/Home"));
            initURLs.add(new URL("https://www.jumia.com.eg/"));
            initURLs.add(new URL("https://www.stc.com.sa/wps/wcm/connect/english/individual/individual"));
            initURLs.add(new URL("http://english.ahram.org.eg/"));
            initURLs.add(new URL("https://hackr.io/blog/what-is-programming"));
            initURLs.add(new URL("https://lol.disney.com/games"));
            initURLs.add(new URL("https://me.ign.com/en/"));
            initURLs.add(new URL("https://www.metacritic.com/browse/games/release-date/available/ps4/metascore"));
            initURLs.add(new URL("https://www.cartoonnetworkhq.com/games"));
            initURLs.add(new URL("https://www.gsmarena.com/apple-phones-48.php"));
            initURLs.add(new URL("https://www.gsmarena.com/"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk03r5Ou5EqKGy-IvAub0JwlSsFUA4Q%3A1589381184187&source=hp&ei=QAi8XpKdCK2alwTJi7qoCA&q=movies&oq=movies&gs_lcp=CgZwc3ktYWIQAzIECCMQJzIECAAQQzIECAAQQzIECAAQQzIECAAQQzIECAAQQzIECAAQQzIECAAQQzICCAAyAggAOgUIABCLAzoICAAQgwEQiwM6BQgAEIMBOgUIABCRAlC0Clj7EWCDE2gAcAB4AIABuAGIAe4GkgEDMC41mAEAoAEBqgEHZ3dzLXdpergBAg&sclient=psy-ab&ved=0ahUKEwiSvZKnirHpAhUtzYUKHcmFDoUQ4dUDCAY&uact=5"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk02NBaskF-CWTg6wKOoXimtLECxELQ%3A1589381187245&ei=Qwi8XqGrDtmU1fAPo_Od4Ao&q=programming&oq=programming&gs_lcp=CgZwc3ktYWIQAzIECCMQJzIFCAAQkQIyBwgAEBQQhwIyAggAMgIIADICCAAyAggAMgIIADICCAAyAggAOgQIABBHOgQIABBDOggIABCDARCLAzoFCAAQiwM6BQgAEIMBUKHLAVi43AFgo90BaABwAngAgAG0AYgB3wySAQQwLjEwmAEAoAEBqgEHZ3dzLXdpergBAg&sclient=psy-ab&ved=0ahUKEwjh2M-oirHpAhVZShUIHaN5B6wQ4dUDCAs&uact=5"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk01_Ul23oRuBOFbCHs3g4Pmp2K2_Ow%3A1589381217809&ei=YQi8Xu30MNaJ1fAPo9S3iAM&q=football&oq=football&gs_lcp=CgZwc3ktYWIQAzIECCMQJzIHCAAQFBCHAjICCAAyAggAMgIIADICCAAyAggAMgIIADICCAAyAggAOgQIABBHOgQIABBDOgcIABCDARBDOgUIABCDAToICAAQgwEQiwM6BQgAEIsDOgUIABCRAlCJeliEiQFgjIsBaABwAngAgAHAAYgBxwmSAQMwLjiYAQCgAQGqAQdnd3Mtd2l6uAEC&sclient=psy-ab&ved=0ahUKEwitqZm3irHpAhXWRBUIHSPqDTEQ4dUDCAs&uact=5"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk02YJitPK8HoXxxA3h9KSmyyhH_yQw%3A1589381236739&ei=dAi8XpjULOvP1fAPsdO7iAU&q=cooking&oq=cooking&gs_lcp=CgZwc3ktYWIQAzIECCMQJzIHCAAQFBCHAjICCAAyAggAMgIIADICCAAyAggAMgIIADICCAAyAggAOgQIABBHOgQIABBDOgUIABCLAzoHCAAQQxCLA1DGhQFYt44BYK2SAWgAcAJ4AIABqwGIAbcIkgEDMC43mAEAoAEBqgEHZ3dzLXdpergBAg&sclient=psy-ab&ved=0ahUKEwiY3pzAirHpAhXrZxUIHbHpDlEQ4dUDCAs&uact=5"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk01_OXakeAGS3A5eFteXrmfI-99rmg%3A1589381256829&ei=iAi8Xr6RMtme1fAP5quraA&q=tennis&oq=tennis&gs_lcp=CgZwc3ktYWIQAzIFCAAQkQIyBQgAEJECMgQIABBDMggIABCDARCLAzIICAAQgwEQiwMyAggAMgIIADICCAAyAggAMgIIADoECAAQRzoECCMQJzoLCAAQgwEQkQIQiwM6CAgAEJECEIsDOgUIABCLA1CKhgFYy48BYPWRAWgAcAJ4AIABtAGIAfwHkgEDMC42mAEAoAEBqgEHZ3dzLXdpergBAw&sclient=psy-ab&ved=0ahUKEwi-9ebJirHpAhVZTxUIHebVCg0Q4dUDCAs&uact=5"));
            initURLs.add(new URL("https://www.google.com/search?safe=active&sxsrf=ALeKk00hRaGKowMoW1esRUWiwKjwIhTJ5w%3A1589381276545&ei=nAi8XvXhIJOi1fAPo9WriAI&q=laptops&oq=laptops&gs_lcp=CgZwc3ktYWIQAzIECAAQQzIECAAQQzIHCAAQFBCHAjICCAAyAggAMgIIADICCAAyAggAMgIIADICCAA6BAgAEEc6BAgjECc6CAgAEIMBEIsDOgUIABCLA1CErAFYurQBYOK4AWgAcAJ4AIABswGIAeoIkgEDMC43mAEAoAEBqgEHZ3dzLXdpergBAg&sclient=psy-ab&ved=0ahUKEwj1n5rTirHpAhUTURUIHaPqCiEQ4dUDCAs&uact=5"));
            WebCrawler wcr = new WebCrawler(initURLs, 20);
        }
        else
        {
            try {
                WebCrawler wcr = new WebCrawler();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

   }

}








