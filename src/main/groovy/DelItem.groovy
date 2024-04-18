/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT054MI.DelItem
 * Description : Delete the item received as a parameter from the assortments and price lists
 * Date         Changed By   Description
 * 20210503     RENARN       TARX02 - Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class DelItem extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  private int currentCompany
  private String itno
  private String ascd
  private String fdat
  private String prrf
  private String cucd
  private String cuno
  private String fvdt
  private String obv1
  private String obv2
  private String obv3
  private String vfdt

  public DelItem(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi;
    this.database = database;
    this.logger = logger;
    this.program = program;
    this.utility = utility;
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO;
    } else {
      currentCompany = mi.in.get("CONO");
    }

    // Check item
    itno = mi.in.get("ITNO")
    if(mi.in.get("ITNO") != null){
      DBAction query = database.table("MITMAS").index("00").build()
      DBContainer MITMAS = query.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO",  mi.in.get("ITNO"))
      if (!query.read(MITMAS)) {
        mi.error("Code article " + mi.in.get("ITNO") + " n'existe pas")
        return
      }
    } else {
      mi.error("Code article est obligatoire")
      return
    }

    // Delete item from assortments
    DBAction OASITN_query = database.table("OASITN").index("20").build();
    DBContainer OASITN = OASITN_query.getContainer();
    OASITN.setInt("OICONO",currentCompany);
    OASITN.set("OIITNO",  mi.in.get("ITNO"))
    if(!OASITN_query.readAll(OASITN, 2, OASITN_outData)){
    }

    // Delete item from price lists
    DBAction OPRBAS_query = database.table("OPRBAS").index("20").build();
    DBContainer OPRBAS = OPRBAS_query.getContainer();
    OPRBAS.setInt("ODCONO",currentCompany);
    OPRBAS.set("ODITNO",  mi.in.get("ITNO"))
    if(!OPRBAS_query.readAll(OPRBAS, 2, OPRBAS_outData)){
    }
  }
  Closure<?> OASITN_outData = { DBContainer OASITN ->
    ascd = OASITN.get("OIASCD")
    fdat = OASITN.get("OIFDAT")
    // Delete item from assortment
    executeCRS105MIDltAssmItem(ascd, itno, fdat)
  }
  private executeCRS105MIDltAssmItem(String ASCD, String ITNO, String FDAT){
    def parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed CRS105MI.DltAssmItem: "+ response.errorMessage)
      } else {
      }
    }
    miCaller.call("CRS105MI", "DltAssmItem", parameters, handler)
  }
  Closure<?> OPRBAS_outData = { DBContainer OPRBAS ->
    prrf = OPRBAS.get("ODPRRF")
    cucd = OPRBAS.get("ODCUCD")
    cuno = OPRBAS.get("ODCUNO")
    fvdt = OPRBAS.get("ODFVDT")
    obv1 = OPRBAS.get("ODOBV1")
    obv2 = OPRBAS.get("ODOBV2")
    obv3 = OPRBAS.get("ODOBV3")
    vfdt = OPRBAS.get("ODVFDT")
    // Delete item from price list
    executeOIS017MIDelBasePrice(prrf, cucd, cuno, fvdt, itno, obv1, obv2, obv3, vfdt)
  }
  private executeOIS017MIDelBasePrice(String PRRF, String CUCD, String CUNO, String FVDT, String ITNO, String OBV1, String OBV2, String OBV3, String VFDT){
    def parameters = ["PRRF": PRRF, "CUCD": CUCD, "CUNO": CUNO, "FVDT": FVDT, "ITNO": ITNO, "OBV1": OBV1, "OBV2": OBV2, "OBV3": OBV3, "VFDT": VFDT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS017MI.DelBasePrice: "+ response.errorMessage)
      } else {
      }
    }
    miCaller.call("OIS017MI", "DelBasePrice", parameters, handler)
  }
}
