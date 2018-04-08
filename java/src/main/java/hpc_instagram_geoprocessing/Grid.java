package hpc_instagram_geoprocessing;

import java.io.Serializable;

public class Grid implements Serializable {
  String name;
  Character column;
  Integer row;
  Float xmin;
  Float xmax;
  Float ymin;
  Float ymax;

  public String toString() {
    return this.name
      + "["
      + Float.toString(this.xmin)
      + Float.toString(this.xmax)
      + Float.toString(this.ymin)
      + Float.toString(this.ymax)
      + "]";
  }

}
