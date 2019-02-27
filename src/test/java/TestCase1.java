package java;

import flink.datastream.state.CreditWashForLR;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import spark.ML.RF.CreditDecisionTree;


public class TestCase1 {
    @BeforeMethod
    public void initialize() {
    }

    @DataProvider(name = "provideNumbers")
    public Object[][] provideData() {

        return new Object[][] { { 10, 20 }, { 100, 110 }, { 200, 210 } };
    }

    @Test(dataProvider = "provideNumbers")
    public void TestNgLearn1(int param1, int param2) {
        System.out.println("this is TestNG test case1, and param1 is:"+param1+"; param2 is:"+param2);
        Assert.assertFalse(false);
    }

    @Test(enabled=true)
    public void testMLCredit() throws InterruptedException{
        //数据生成路径文件清除:
        String FlinkInPath="./CreditOrigData";
        String FlinkOutPath="./CreditDataWashedFraudForLR";
        //PathClear(FlinkOutPath);

        //flink实时数据处理
        try {
            CreditWashForLR.main(new String[]{FlinkInPath});
        } catch (Exception e) {
            e.printStackTrace();
        }

        //数据生成检查: 文件是否生成？
        //PathCheck(FlinkOutPath);

        //spark 建模&预测
        CreditDecisionTree.main(new String[]{FlinkOutPath});
        //ScalaObjectDemo.main(new String[]{});

        //模型性能数据检查（上一步骤已经把数据保存到DB/文件中）
        //...
    }

    @Test(dependsOnMethods= {"TestNgLearn1"})
    public void TestNgLearn2() {
        System.out.println("this is TestNG test case2");
    }
}