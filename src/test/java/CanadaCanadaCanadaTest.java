import org.junit.Test;

public class CanadaCanadaCanadaTest {
    @Test
    public void TaskA() throws Exception{
        //1 second
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskA";

        CanadaCanadaCanada.main(input);
    }
    @Test
    public void TaskB() throws Exception{
        //20 seconds
        String[] input = new String[4];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskB1TEMP";
        input[3] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskB2";


        TaskB.main(input);
    }

    @Test
    public void TaskC() throws Exception{
        //1 second
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskC";

        TaskC.main(input);
    }
    @Test
    public void TaskD() throws Exception{
        //2 seconds
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskD";

        TaskD.main(input);
    }
    @Test
    public void TaskE() throws Exception{
        //24 Seconds
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskE";

        TaskE.main(input);
    }
    @Test
    public void TaskF() throws Exception{
        //24 Seconds
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskF";

        TaskF.main(input);
    }
    @Test
    public void TaskG() throws Exception{
        //20 Seconds
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskG";

        TaskG.main(input);
    }
    @Test
    public void TaskH() throws Exception{
        //1 Second
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskH";

        TaskH.main(input);
    }



}
