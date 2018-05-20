import java.io.Serializable;

/* Geographical grid boxes */
public class Grid implements Serializable {
  String name;
  String column;
  String row;
  Float xmin;
  Float xmax;
  Float ymin;
  Float ymax;

  /* Checks if a point is in this grid */
  public Boolean inGrid(Float x, Float y) {
    return(x >= xmin && x <= xmax && y >= ymin && y <= ymax);
  }

}
