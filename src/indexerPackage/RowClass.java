package indexerPackage;

import java.util.StringTokenizer;

public class RowClass {
    public String URL;
    public String Word;
    public String imageSrc;
    public String bodyPrev;
    public int Count;
    public int H1Count;
    public int H2Count;
    public int H3Count;
    public int H4Count;
    public int H5Count;
    public int H6Count;
    public int BoldCount;
    public int ItalicCount;
    public int TitleCount;

    public void print(){
        System.out.println("URL = "+URL+"  Word = "+Word+"  Count = "+Count+" H1Count = "+H1Count+"  H2Count = "+ H2Count+"  H3Count = "+H3Count+ "  H4Count = "+H4Count+"  H5Count = "+H5Count+"  H6Count = "+H6Count+"  BoldCount = "+BoldCount + "  ItalicCount = " + ItalicCount+ "  Title Count = "+TitleCount);
    }
}
