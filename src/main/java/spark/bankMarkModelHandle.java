package spark;

import org.apache.spark.network.client.TransportClient;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class bankMarkModelHandle {
    public static void ClientInfo2Map(){

    }

//    public static void ESputData(TransportClient client, ModelInfo  modelInfo){
//        try{
//            Map<String, Object> map = new HashMap<>();
//            map.put("age", modelInfo.getName());
//            map.put("job", modelInfo.getVersion());
//            map.put("marital", modelInfo.getAccuracy());
//            map.put("education",modelInfo.getEducation());
//            //...
//
//            Date now = new Date( );
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
//            IndexResponse response = client.prepareIndex("bank_market", sdf.format(now)).setSource(map).get();
//            System.out.println(response.getId());
////            logger.info("map索引名称:" + response.getIndex() + "\n map类型:" + response.getType()
////                    + "\n map文档ID:" + response.getId() + "\n当前实例map状态:" + response.status());
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public static void main(String[] args) {
//        EsClient cl =new EsClient("my-application","127.0.0.1");
//        ClientInfo cInfo=new ClientInfo();
//        cInfo.setAge("33");
//        cInfo.setJob("technician");
//        cInfo.setEducation("university.degree");
//        cInfo.setMarital("single");
//
//        ESputData(cl.getConnection(),cInfo);
//    }
}