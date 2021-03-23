import com.sy.util.PropertiesReader;

import java.util.UUID;
/**
 * Created by YanShi on 2020/7/31 1:42 下午
 */
public class Test {
    public static void main(String[] args) {
        String uuid = UUID.randomUUID().toString();
        System.out.println(uuid);
        System.out.println(uuid.replace("-", ""));
    }


}
