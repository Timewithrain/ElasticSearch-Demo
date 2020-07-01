package com.watermelon.util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class HtmlParseUtil {

    public static void parse() throws IOException {
        String url = "http://search.jd.com/Search?keyword=java";
        Document document = Jsoup.parse(new URL(url),5000);
        Element element = document.getElementById("J_goodsList");
        Elements elements = element.getElementsByTag("li");
        for (Element ele : elements){
            String price = ele.getElementsByClass("p-price").eq(0).text();
            String title = ele.getElementsByClass("p-name").eq(0).text();
            System.out.println(title+":"+price);
            System.out.println("===========");
        }
    }

}
