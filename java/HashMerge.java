import java.util.*;
import mpi.*;

class HashMerge extends User_function {

  HashMerge() {
  }

  @SuppressWarnings("unchecked")
  public void Call(Object invec, int inoffset, Object outvec, int outoffset, int count, Datatype datatype) {
    Object[] in_array = (Object[]) invec;
    Object[] out_array = (Object[]) outvec;

    HashMap<String, Integer> x = (HashMap<String, Integer>) out_array[0];
    HashMap<String, Integer> y = (HashMap<String, Integer>) in_array[0];

    for (Map.Entry<String, Integer> entry: y.entrySet()) {
     String name = entry.getKey();
     Integer ycount = entry.getValue();
     Integer xcount = 0;
     if (x.containsKey(name)) {
       xcount = x.get(name);
     }
     x.put(name, xcount + ycount);
    }
  }
}
