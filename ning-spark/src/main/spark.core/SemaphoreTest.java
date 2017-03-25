import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by ning on 2016/9/2.
 */
public class SemaphoreTest {
    static Semaphore sem = new Semaphore(0);
    public static void main(String[] args) throws Exception{
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i = 0 ;i < 5 ;i++){
                    sem.release();
                    System.out.println("释放一个许可");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        t1.setDaemon(true);
        t1.start();
        System.out.println("主线程获取5个许可");
        boolean b =  sem.tryAcquire(5,3, TimeUnit.SECONDS);

        System.out.println("主线程结束,b="+b);
    }
}
