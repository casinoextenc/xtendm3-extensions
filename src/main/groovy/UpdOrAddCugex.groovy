/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT870MI.UpdOrAddCugex
 * Description : The AddOrUpdCugex add or upd CUGEX1.
 * Date         Changed By   Description
 * 20211027     APACE        COMX01 - Management of customer agreement
 * 20220128     APACE        CUSEXTMI.AddFieldValue and ChgFieldValue have been added
 * 20220208     MBEN         Adding input for N296 / N396 /  A830
 * 20220222     CDUV         Chg N196
 * 20221124     MBEN         Removing mandatory fields PK02 and PK03
 * 20230224     SEAR         Add To EXT870MI
 * 20240701     PBEAUDOUIN   For validation Xtend
 */
public class UpdOrAddCugex extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  Integer currentCompany = 0
  String file = ''
  String pk01 = ''
  String pk02 = ''
  String pk03 = ''
  String iN196 = '' //A° 20220222     CDUV

  public UpdOrAddCugex(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }
  //main : insert or update data in CUGEX1 table with CUEXTMI.AddFieldValue or ChgFieldValue.
  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    if (mi.in.get("FILE") != null) {
      file = mi.in.get("FILE")
    }
    if (mi.in.get("PK01") != null) {
      pk01 = mi.in.get("PK01")
    }
    DBAction query = database.table("CUGEX1").index("00").selection('F1FILE', 'F1PK01', 'F1PK02', 'F1PK03', 'F1A030',
      'F1A121', 'F1A130', 'F1A230', 'F1A330', 'F1A430', 'F1A530',
      'F1A630', 'F1A730', 'F1A830', 'F1A930', 'F1N096', 'F1N196', 'F1N396').build()
    DBContainer container = query.getContainer()
    container.set("F1CONO", currentCompany)
    container.set("F1FILE", file)
    container.set("F1PK01", pk01)
    container.set("F1PK02", pk02)
    container.set("F1PK03", pk03)
    if (!query.read(container)) {
      Map<String, String> params = ["FILE": file, "PK01": pk01, "PK02": pk02,
                                    "PK03": pk03]

      if (mi.in.get("A030") != null) {
        params.put("A030", mi.in.get("A030").toString())
      }
      if (mi.in.get("A121") != null) {
        params.put("A121", mi.in.get("A121").toString())
      }
      if (mi.in.get("A130") != null) {
        params.put("A130", mi.in.get("A130").toString())
      }
      if (mi.in.get("A230") != null) {
        params.put("A230", mi.in.get("A230").toString())
      }
      if (mi.in.get("A330") != null) {
        params.put("A330", mi.in.get("A330").toString())
      }
      if (mi.in.get("A430") != null) {
        params.put("A430", mi.in.get("A430").toString())
      }
      if (mi.in.get("A530") != null) {
        params.put("A530", mi.in.get("A530").toString())
      }
      if (mi.in.get("A630") != null) {
        params.put("A630", mi.in.get("A630").toString())
      }
      if (mi.in.get("A730") != null) {
        params.put("A730", mi.in.get("A730").toString())
      }
      if (mi.in.get("A830") != null) {
        params.put("A830", mi.in.get("A830").toString())
      }
      if (mi.in.get("A930") != null) {
        params.put("A930", mi.in.get("A930").toString())
      }
      if (mi.in.get("N096") != null) {
        params.put("N096", mi.in.get("N096").toString())
      }
      iN196 = mi.in.get("N196") as Integer //A° 20220222     CDUV
      if (mi.in.get("N196") != null) {
        params.put("N196", iN196)//A° 20220222     CDUV
      }
      if (mi.in.get("N296") != null) {
        params.put("N296", mi.in.get("N296").toString())

      }
      if (mi.in.get("N396") != null) {
        params.put("N396", mi.in.get("N396").toString())

      }
      Closure<?> closure = { Map<String, String> response ->
      }
      miCaller.call("CUSEXTMI", "AddFieldValue", params, closure)
    } else {
      Map<String, String> params = ["FILE": file, "PK01": pk01, "PK02": pk02,
                                    "PK03": pk03]
      if (mi.in.get("A030") != null) {
        params.put("A030", mi.in.get("A030").toString())
      }
      if (mi.in.get("A121") != null) {
        params.put("A121", mi.in.get("A121").toString())
      }
      if (mi.in.get("A130") != null) {
        params.put("A130", mi.in.get("A130").toString())
      }
      if (mi.in.get("A230") != null) {
        params.put("A230", mi.in.get("A230").toString())
      }
      if (mi.in.get("A330") != null) {
        params.put("A330", mi.in.get("A330").toString())
      }
      if (mi.in.get("A430") != null) {
        params.put("A430", mi.in.get("A430").toString())
      }
      if (mi.in.get("A530") != null) {
        params.put("A530", mi.in.get("A530").toString())
      }
      if (mi.in.get("A630") != null) {
        params.put("A630", mi.in.get("A630").toString())
      }
      if (mi.in.get("A730") != null) {
        params.put("A730", mi.in.get("A730").toString())
      }
      if (mi.in.get("A830") != null) {
        params.put("A830", mi.in.get("A830").toString())
      }
      if (mi.in.get("A930") != null) {
        params.put("A930", mi.in.get("A930").toString())
      }
      if (mi.in.get("N096") != null) {
        params.put("N096", mi.in.get("N096").toString())
      }
      iN196 = mi.in.get("N196") as Integer //A° 20220222     CDUV

      if (mi.in.get("N196") != null) {
        params.put("N196", iN196)
      }
      if (mi.in.get("N296") != null) {
        params.put("N296", mi.in.get("N296").toString())
      }
      if (mi.in.get("N396") != null) {
        params.put("N396", mi.in.get("N396").toString())
      }
      Closure<?> closure = { Map<String, String> response ->
      }
      miCaller.call("CUSEXTMI", "ChgFieldValue", params, closure)
    }
  }
}
