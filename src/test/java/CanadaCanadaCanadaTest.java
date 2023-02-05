import org.junit.Test;

public class CanadaCanadaCanadaTest {
    @Test
    public void TaskA() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskA";

        CanadaCanadaCanada amazing = new CanadaCanadaCanada();
        amazing.debug(input);
    }
    @Test
    public void TaskB() throws Exception{
        String[] input = new String[4];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskB1TEMP";
        input[3] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskB2";


        TaskB amazing = new TaskB();
        amazing.main(input);
    }

    @Test
    public void TaskC() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskC";

        TaskC amazing = new TaskC();
        amazing.debug(input);
    }
    @Test
    public void TaskD() throws Exception{
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskD";

        TaskD amazing = new TaskD();
        amazing.debug(input);
    }
    @Test
    public void TaskE() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskE";

        TaskE amazing = new TaskE();
        amazing.main(input);
    }
    @Test
    public void TaskF() throws Exception{
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskF";

        TaskF amazing = new TaskF();
        amazing.main(input);
    }
    @Test
    public void TaskG() throws Exception{
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskG";


        TaskG amazing = new TaskG();
        amazing.debug(input);
    }
    @Test
    public void TaskH() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output/taskH";

        TaskH amazing = new TaskH();
        amazing.main(input);
    }



}
