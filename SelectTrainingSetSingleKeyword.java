/**Created by LG
 Training Set Selector
 reviewType: quality, appearance, durability, shipment, image reliability
 Single keyword

 10/27/2017
 **/
import java.io.*;
import java.text.ParseException;
import java.util.Arrays;


import org.json.JSONException;
import org.json.JSONObject;



public class SelectTraningSetSingleKeyword {
    public static void main(String[] args) throws IOException, JSONException, ParseException {

        //Open existing file and write into a new file
        File file = new File("C:\\WorkSpace\\Pet_Supplies_5.json");
        BufferedReader br = new BufferedReader(new FileReader("Pet_Supplies_5.json"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("Pet_Supplies_5-SK-TS.json"));

        String tempString = null;
        int countQuality = 0, countAppearance = 0, countDurability = 0, countShipment = 0, countImageReliability = 0, countOther = 0, countTOT = 0;
        int keywordIndex[]= new int[5];

        while ((tempString = br.readLine()) != null){
            TrainingSetFilter trainingSet = new TrainingSetFilter();
            JSONObject jsonRecord = new JSONObject(tempString);
            String jsonText = jsonRecord.getString("reviewText");

            //Keywords for 5 types
            keywordIndex[0] = jsonText.indexOf("quality");
            keywordIndex[1] = jsonText.indexOf("look");
            keywordIndex[2] = jsonText.indexOf("durab");
            keywordIndex[3] = jsonText.indexOf("ship");
            keywordIndex[4] = jsonText.indexOf("picture");
            int min_keywordIndex = 1000000;
            for(int i = 0; i < keywordIndex.length; i++){
                if(keywordIndex[i] != -1){
                    if(keywordIndex[i] < min_keywordIndex){
                        min_keywordIndex = keywordIndex[i];
                    }
                }
            }
            if(keywordIndex[0] != -1 && keywordIndex[0] <= min_keywordIndex){
                jsonRecord.put("reviewType", "quality");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countQuality++;
            }
            else if(keywordIndex[1] != -1 && keywordIndex[1] <= min_keywordIndex){
                jsonRecord.put("reviewType", "appearance");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countAppearance++;
            }
            else if(keywordIndex[2] != -1 && keywordIndex[2] <= min_keywordIndex){
                jsonRecord.put("reviewType", "durability");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countDurability++;
            }
            else if(keywordIndex[3] != -1 && keywordIndex[3] <= min_keywordIndex){
                jsonRecord.put("reviewType", "shipment");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countShipment++;
            }
            else if(keywordIndex[4] != -1 && keywordIndex[4] <= min_keywordIndex){
                jsonRecord.put("reviewType", "image reliability");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countImageReliability++;
            }
            else if(keywordIndex[0] == -1 && keywordIndex[1] == -1 && keywordIndex[2] == -1 && keywordIndex[3] == -1 && keywordIndex[4] == -1){
                jsonRecord.put("reviewType", "other");
                bw.write(jsonRecord.toString());
                bw.write("\n");
                countOther++;
            }
            bw.flush();
        }

        countTOT = countAppearance + countDurability + countImageReliability + countQuality + countShipment + countOther;
        System.out.println("Number of reviews in Training Set: " + countTOT);
        System.out.println("Number of reviews labeled Quality: " + countQuality);
        System.out.println("Number of reviews labeled Appearance: " + countAppearance);
        System.out.println("Number of reviews labeled Durability: " + countDurability);
        System.out.println("Number of reviews labeled Shipment: " + countShipment);
        System.out.println("Number of reviews labeled Image Reliability: " + countImageReliability);
        System.out.println("Number of reviews labeled Other: " + countOther);
        br.close();
        bw.close();

    }



}
