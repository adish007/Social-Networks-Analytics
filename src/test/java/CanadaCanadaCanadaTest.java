import org.junit.Test;

public class CanadaCanadaCanadaTest {
    @Test
    public void TaskA() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output";

        CanadaCanadaCanada amazing = new CanadaCanadaCanada();
        amazing.debug(input);
    }
    @Test
    public void TaskC() throws Exception{
        String[] input = new String[2];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output";

        TaskC amazing = new TaskC();
        amazing.debug(input);
    }
    @Test
    public void TaskD() throws Exception{
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/Friends.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output";

        TaskD amazing = new TaskD();
        amazing.debug(input);
    }

    @Test
    public void TaskG() throws Exception{
        String[] input = new String[3];
        input[0] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/MyPage.csv";
        input[1] = "/home/aidan/codinShit/CS4433Project1WORKING/java/src/main/resources/AccessLog.csv";
        input[2] = "/home/aidan/codinShit/CS4433Project1WORKING/java/output";


        TaskG amazing = new TaskG();
        amazing.debug(input);
    }
}
