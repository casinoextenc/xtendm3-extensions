import java.math.RoundingMode

/**
 * Name : EXT011MI.GetORTPByValues Version 1.0
 * Api to apply specific rules for rounding order qty
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20221212     FLEBARS       Creation EXT011
 */
public class GetORTPByValues extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public GetORTPByValues(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  /**
   * Initialize variables
   * Get Customer informations
   * Get Item informations
   * Get Item informations
   */
  public void main() {
    //Initializing vars
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String a030 = (String)(mi.in.get("A030") != null ? mi.in.get("A030") : "")
    String a130 = (String)(mi.in.get("A130") != null ? mi.in.get("A130") : "")
    int chb1 = (Integer)(mi.in.get("CHB1") != null ? mi.in.get("CHB1") : 0)
    int chb2 = (Integer)(mi.in.get("CHB2") != null ? mi.in.get("CHB2") : 0)

    String customerCode = (mi.in.get("A030") != null ? (String)mi.in.get("A030") : "")
    String supplierCode = (mi.in.get("A130") != null ? (String)mi.in.get("A130") : "")
    String pickupDate = (mi.in.get("CHB1") != null ? (Integer)mi.in.get("CHB1") : 0)
    String pickuptHour = (mi.in.get("CHB2") != null ? (Integer)mi.in.get("CHB2") : 0)

    ExpressionFactory expression = database.getExpressionFactory("CUGEX1")
    expression = expression.eq("F1A030", customerCode)
        .and(expression.eq("F1A130", supplierCode))
        .and(expression.eq("F1CHB1", pickupDate))
        .and(expression.eq("F1CHB2", pickuptHour))

    DBAction queryCUGEX1 = database.table("CUGEX1").index("00")
        .matching(expression).selection("F1CONO",
        "F1FILE",
        "F1PK01"
        ).build()

    DBContainer containerCUGEX1 = queryCUGEX1.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "OOTYPE")

    //Record exists
    if (!queryCUGEX1.readAll(containerCUGEX1, 1, 1,outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> outData = { DBContainer containerCUGEX1 ->
    String OrderType = containerCUGEX1.get("F1PK01")
    mi.outData.put("ORTP", OrderType)
    mi.write()
  }
}
