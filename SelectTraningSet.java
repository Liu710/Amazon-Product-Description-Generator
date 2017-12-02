/**Created by LG
 Training Set Selector
 reviewType: quality, appearance, durability, shipment, image reliability
Multiple keywords for labelling each type

 10/27/2017
 **/
import java.io.*;
import java.text.ParseException;
import java.util.Arrays;


import org.json.JSONException;
import org.json.JSONObject;



public class SelectTraningSet {
    public static void main(String[] args) throws IOException, JSONException, ParseException {

        //Open existing data file and write into new file
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

            //Type: Quality~keyowrdIndex[0]
           int qualityIndex[] = new int[3];
           qualityIndex[0] = jsonText.indexOf("perform");
           qualityIndex[1] = jsonText.indexOf("quality");
           qualityIndex[2] = jsonText.indexOf("work");
           Arrays.sort(qualityIndex);
           if(Arrays.stream(qualityIndex).max().getAsInt() == -1){
               keywordIndex[0] = -1;
           }
           else if(Arrays.stream(qualityIndex).max().getAsInt() >= 0){
               for(int i = 0; i < qualityIndex.length; i++){
                   if(qualityIndex[i] >= 0){
                       keywordIndex[0] = qualityIndex[i];
                       break;
                   }
               }
           }


            //Type: Appearance~keywordIndex[1]
           int AppearanceIndex[] = new int[4];
           AppearanceIndex[0] = jsonText.indexOf("look");
           AppearanceIndex[1] = jsonText.indexOf("beautiful");
           AppearanceIndex[2] = jsonText.indexOf("ulgy");
           AppearanceIndex[3] = jsonText.indexOf("appear");
           Arrays.sort(AppearanceIndex);
           if(Arrays.stream(AppearanceIndex).max().getAsInt() == -1){
               keywordIndex[1] = -1;
           }
           else if(Arrays.stream(AppearanceIndex).max().getAsInt() >= 0){
               for(int i = 0; i < AppearanceIndex.length; i++){
                   if(AppearanceIndex[i] >= 0){
                       keywordIndex[1] = AppearanceIndex[i];
                       break;
                   }
               }
           }

           //Type: Durability~keywordIndex[2]
           int DurabilityIndex[] = new int[4];
           DurabilityIndex[0] = jsonText.indexOf("durab");
           DurabilityIndex[1] = jsonText.indexOf("week");
           DurabilityIndex[2] = jsonText.indexOf("month");
           DurabilityIndex[3] = jsonText.indexOf("stop");
           Arrays.sort(DurabilityIndex);
           if(Arrays.stream(DurabilityIndex).max().getAsInt() == -1){
               keywordIndex[2] = -1;
           }
           else if(Arrays.stream(DurabilityIndex).max().getAsInt() >= 0){
               for(int i = 0; i < DurabilityIndex.length; i++){
                   if(DurabilityIndex[i] >= 0){
                       keywordIndex[2] = DurabilityIndex[i];
                       break;
                   }
               }
           }


           //Type: Shipment~keywordIndex[3]
           int shipmentIndex[] = new int[2];
           shipmentIndex[0] = jsonText.indexOf("shipping");
           shipmentIndex[1] = jsonText.indexOf("ship");
           Arrays.sort(shipmentIndex);
           if(Arrays.stream(shipmentIndex).max().getAsInt() == -1){
               keywordIndex[3] = -1;
           }
           else if(Arrays.stream(shipmentIndex).max().getAsInt() >= 0){
               for(int i = 0; i < shipmentIndex.length; i++){
                   if(shipmentIndex[i] >= 0){
                       keywordIndex[3] = shipmentIndex[i];
                       break;
                   }
               }
           }

           //Type: Image Reliability~keywordIndex[4]
           int imageReliabilityIndex[] = new int[2];
           imageReliabilityIndex[0] = jsonText.indexOf("picture");
           imageReliabilityIndex[1] = jsonText.indexOf("image");
           Arrays.sort(imageReliabilityIndex);
           if(Arrays.stream(imageReliabilityIndex).max().getAsInt() == -1){
               keywordIndex[4] = -1;
           }
           else if(Arrays.stream(imageReliabilityIndex).max().getAsInt() >= 0){
               for(int i = 0; i < imageReliabilityIndex.length; i++){
                   if(imageReliabilityIndex[i] >= 0){
                       keywordIndex[4] = imageReliabilityIndex[i];
                       break;
                   }
               }
           }



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
