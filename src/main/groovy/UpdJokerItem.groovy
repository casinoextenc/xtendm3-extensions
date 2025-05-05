/****************************************************************************************
 Extension Name : EXT050MI.UpdJokerItem
 Type : ExtendM3Transaction
 Author : SEAR
 Description
 This extension is used by Mashup
 List files and containers

 Description : List pallet
 Date         Changed By   Description
 20230601     SEAR         LOG28 - Creation of files and containers
 20250428     FLEBARS      Code review for infor validation
 20250505     FLEBARS      Apply xtendm3 team remarks
 ******************************************************************************************/


import java.time.LocalDateTime

public class UpdJokerItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private String bjnoInput
  private String itnoInput
  private double zquvInput
  private double zpqaInput
  private double mitaunCofa
  private int dmcf
  private String mitmasPuun
  private double palQuantity
  private double totQuantity
  private double totWeight
  private double totVolume
  private double mitmasWeight
  private double mitmasVolume
  private int currentCompany
  private Integer nbMaxRecord = 10000

  public UpdJokerItem(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    bjnoInput = (mi.in.get("BJNO") != null ? (String)mi.in.get("BJNO") : "")
    itnoInput = (mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : "")
    zquvInput = (double) (mi.in.get("ZQUV") != null ? mi.in.get("ZQUV") : 0)
    zpqaInput = (double) (mi.in.get("ZPQA") != null ? mi.in.get("ZPQA") : 0)

    //Check if record exists in Constraint Type Table (EXT055)
    DBAction ext055Query = database.table("EXT055").index("00").build()
    // list out data
    DBAction ext055Request = database.table("EXT055").index("00").selection("EXBJNO").build()
    DBContainer ListContainerEXT055 = ext055Request.getContainer()
    ListContainerEXT055.set("EXBJNO", bjnoInput)
    //Record exists
    if (!ext055Request.readAll(ListContainerEXT055, 1, nbMaxRecord, ext055Reader)){
      mi.error("Numéro de job " + bjnoInput + " n'existe pas dans la table EXT055")
      return
    }

    //Check if Item exist
    mitmasPuun = ""
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMPUUN","MMVOL3","MMGRWE","MMSUNO").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", itnoInput)
    if(!mitmasQuery.read(mitmasRequest)){
      mi.error("Code article " + itnoInput + " n'existe pas")
    } else {
      mitmasPuun = mitmasRequest.get("MMPUUN")
      mitmasVolume = mitmasRequest.getDouble("MMVOL3")
      mitmasWeight = mitmasRequest.getDouble("MMGRWE")
    }

    DBAction ext055LQuery = database.table("EXT055")
      .index("00")
      .selection(
        "EXBJNO",
        "EXCONO",
        "EXITNO",
        "EXITDS",
        "EXZAV1",
        "EXZAV2",
        "EXZQUV",
        "EXZPQA",
        "EXGRWE",
        "EXVOL3",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext055LRequest = ext055LQuery.getContainer()
    ext055LRequest.set("EXBJNO", bjnoInput)
    ext055LRequest.set("EXCONO", currentCompany)
    ext055LRequest.set("EXITNO", itnoInput)
    if(!ext055LQuery.readLock(ext055LRequest, ext055Updater)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  /**
   * Update EXT055
   */
  Closure<?> ext055Updater = { LockedResult ext055LockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = ext055LockedResult.get("EXCHNO")

    DBAction mitaunQuery = database.table("MITAUN").index("00").selection(
      "MUCONO",
      "MUITNO",
      "MUAUTP",
      "MUALUN",
      "MUCOFA",
      "MUDMCF"
    ).build()

    totWeight = 0
    totVolume = 0
    palQuantity = zpqaInput
    totQuantity = 0
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itnoInput)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", mitmasPuun)
    if (mitaunQuery.read(mitaunRequest)) {
      mitaunCofa = mitaunRequest.getDouble("MUCOFA")
      dmcf = mitaunRequest.getInt("MUDMCF")
      if (dmcf == 1) {
        palQuantity = zpqaInput * mitaunCofa
      } else {
        palQuantity = zpqaInput / mitaunCofa
      }
    }

    totQuantity = palQuantity + zpqaInput
    totWeight = totQuantity * mitmasWeight
    totVolume = totQuantity * mitmasVolume
    logger.debug("totQuantity : " + totQuantity)
    logger.debug("totWeight : " + totWeight)
    logger.debug("totVolume : " + totVolume)
    logger.debug("mitaunCofa : " + mitaunCofa)
    logger.debug("dmcf : " + dmcf)

    ext055LockedResult.set("EXZQUV", zquvInput)
    ext055LockedResult.set("EXZPQA", palQuantity)
    ext055LockedResult.set("EXGRWE", totWeight)
    ext055LockedResult.set("EXVOL3", totVolume)
    ext055LockedResult.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
    ext055LockedResult.setInt("EXCHNO", ((Integer)ext055LockedResult.get("EXCHNO") + 1))
    ext055LockedResult.set("EXCHID", program.getUser())
    ext055LockedResult.update()
  }

  /**
   * Get EXT055 data
   */
  Closure<?> ext055Reader = { DBContainer ext055Result ->
    return
  }

}
