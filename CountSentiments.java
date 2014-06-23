package myudfs;
import java.io.*;
import java.util.*;

 
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;



//Input: a tuple containing Key-value pair: Key: Job_ID, Value: Bag of tokens
//Output: a tuple containing (Job_ID, (word1, word2, word3....)) After a series of operations

public class CountSentiments extends EvalFunc<String>{


	//Add a list of distributed cache files
	public List<String> getCacheFiles(){
		List<String> list = new ArrayList<String>(2); 
        list.add("/user/huser38/Homework5/tweets/bad.txt#BadWords"); 
        list.add("/user/huser38/Homework5/tweets/good.txt#GoodWords");
        return list; 
	}

	//For each records count the sentiments
	
	public String exec(Tuple input) throws IOException{
		
		if(input == null){
			return null;
		}

		
		
		//Add all the good words into an arraylist
		FileReader fr = new FileReader("./GoodWords");
		BufferedReader d = new BufferedReader(fr);

		List<String> GoodWordsList = new ArrayList<String>();
		String thisLine;
		while((thisLine = d.readLine()) != null){
			GoodWordsList.add(thisLine);
		}

		fr.close();

		//Add all the bad words into an arrayList
		FileReader fr2 = new FileReader("./BadWords");
		BufferedReader d2 = new BufferedReader(fr2);

		List<String> BadWordsList = new ArrayList<String>();

		String thisLine2;
		while((thisLine2 = d2.readLine()) != null){
			BadWordsList.add(thisLine2);
		}

		fr2.close();


		//Create two records counting good and bad words
		int num_of_positive = 0;
		int num_of_negative = 0;


		//Actual Processing step starts
		String line = (String)input.get(0);

		String[] strList = line.split(" ");
		String word;
		//get rid of those 
		for(int i = 0; i < strList.length; i++){
			//strList[i] = strList[i].replaceAll("[^A-Za-z]", "");
			word = strList[i].toLowerCase();

			if(GoodWordsList.contains(word)){
				num_of_positive++;
			}
			else if(BadWordsList.contains(word)){
				num_of_negative++;
			}
		}


		int sentiment_score = num_of_positive - num_of_negative;

		String indicator = "Neutral";
		if(sentiment_score > 0){
			indicator = "Positive";
			return indicator;
		}
		else if(sentiment_score < 0){
			indicator = "Negative";
			return indicator;
		}
		else{
			return indicator;
		}
	}
}
