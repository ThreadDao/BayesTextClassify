import task.Task;
import task.impl.BayesTextClassifyTask;

public class Main {
    public static void main(String[] args) throws Exception {
        Task task = new BayesTextClassifyTask();
        task.setProfile("local");
        task.process(args);
    }
}
