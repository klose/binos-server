package com.klose.common;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.klose.common.TransformerIO.FileUtililty;
import com.klose.common.TransformerIO.FileUtililty.FStype;

/**
 * Parser the xml.
 * It unifies the different implementation about parsing the XML. 
 * Developer can adjust the method of parsing xml, 
 * only need to override the function getDocument().  
 * Default xml parser  is DOM4J.
 * @author Bing Jiang
 *
 */
public class XMLParser {
	private FStype type;
	private String path;
	private Document doc = null;
	private static final Logger LOG = Logger.getLogger(XMLParser.class.getName());
	public XMLParser(String path) {
		this.path = path;
		if(!FileUtililty.checkFileValid(path)) {
			LOG.log(Level.SEVERE, "Can't find the XML path:"+path);
		}
		this.type = FileUtililty.getFileType(path);
	}
	
	/*Use the DOM4j to return the document of XML, no matter where is xml. 
	 * */
	public Document getDocument() {	
		if(doc != null) {
			return doc;
		}
		SAXReader reader = new SAXReader();
		try {
			if(this.type == FStype.HDFS) {
				Configuration conf  = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				Path p = new Path(this.path);
				doc = reader.read(fs.open(p));
			}
			else if(this.type == FStype.LOCAL) {
				File file = new File(this.path);
				doc = reader.read(file);
			}
			else {
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return doc;
	}
	
	public Element getRootElement() {
		return getDocument().getRootElement();
	}

}
