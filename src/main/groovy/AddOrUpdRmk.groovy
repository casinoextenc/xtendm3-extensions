/****************************************************************************************
 Extension Name: EXT013MI.AddOrUpdRmk
 Type: ExtendM3Transaction
 Script Author: FLEBARS
 Date: 2023-03-29
 Description:
 * This API method adds or updates records in the specific table EXT013.

 Revision History:
 Name                    Date             Version          Description of Changes
 FLEBARS                 2023-03-29       1.0              CMD08 - Rapport d'intégration de demande
 ARENARD                 2025-04-10       1.1              Extension has been fixed
 ARENARD                 2025-04-25       1.2              Added check for order line number
 ******************************************************************************************/

public class AddOrUpdRmk extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage

  public AddOrUpdRmk(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in
   * Serialize in
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String orno = (String)(mi.in.get("ORNO") != null ? mi.in.get("ORNO") : "")
    int ponr = (Integer)(mi.in.get("PONR") != null ? mi.in.get("PONR") : 0)
    int posx = (Integer)(mi.in.get("POSX") != null ? mi.in.get("POSX") : 0)
    int lino = (Integer)(mi.in.get("LINO") != null ? mi.in.get("LINO") : 0)
    String fitn = (String)(mi.in.get("FITN") != null ? mi.in.get("FITN") : "")
    String remk = (String)(mi.in.get("REMK") != null ? mi.in.get("REMK") : "")
    String mscd = (String)(mi.in.get("MSCD") != null ? mi.in.get("MSCD") : "")

    // Check order number
    if(mi.in.get("ORNO") != null && mi.in.get("ORNO") != ""){
      DBAction queryOXHEAD = database.table("OXHEAD").index("00").build();
      DBContainer OXHEAD = queryOXHEAD.getContainer();
      OXHEAD.set("OACONO", currentCompany);
      OXHEAD.set("OAORNO", mi.in.get("ORNO"));
      if(!queryOXHEAD.read(OXHEAD)){
        mi.error("La commande " + mi.in.get("ORNO") + " n'existe pas")
        return;
      }
    } else {
      mi.error("Le N° de commande est obligatoire")
      return;
    }

    // Check order line
    if(mi.in.get("PONR") != null && mi.in.get("PONR") != ""){
      DBAction queryOXLINE = database.table("OXLINE").index("00").build();
      DBContainer OXLINE = queryOXLINE.getContainer();
      OXLINE.set("OBCONO", currentCompany);
      OXLINE.set("OBORNO", mi.in.get("ORNO"));
      OXLINE.set("OBPONR", mi.in.get("PONR"));
      OXLINE.set("OBPOSX", mi.in.get("POSX"));
      if(!queryOXLINE.read(OXLINE)){
        mi.error("La ligne de commande " + (mi.in.get("PONR") as Integer) + " n'existe pas")
        return;
      }
    } else {
      mi.error("Le N° de ligne de commande est obligatoire")
      return;
    }

    //Check if record exists
    DBAction queryEXT013 = database.table("EXT013")
      .index("00")
      .selection(
        "EXCONO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXLINO",
        "EXFITN",
        "EXMSCD",
        "EXREMK",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer containerEXT013 = queryEXT013.getContainer()
    containerEXT013.set("EXCONO", currentCompany)
    containerEXT013.set("EXORNO", orno)
    containerEXT013.set("EXPONR", ponr)
    containerEXT013.set("EXPOSX", posx)
    containerEXT013.set("EXLINO", lino)

    //Record exists
    if (queryEXT013.read(containerEXT013)) {
      Closure<?> updateEXT013 = { LockedResult lockedResultEXT013 ->
        lockedResultEXT013.set("EXORNO", orno)
        lockedResultEXT013.set("EXPONR", ponr)
        lockedResultEXT013.set("EXPOSX", posx)
        lockedResultEXT013.set("EXLINO", lino)
        lockedResultEXT013.set("EXFITN", fitn)
        lockedResultEXT013.set("EXREMK", remk)
        lockedResultEXT013.set("EXMSCD", mscd)
        lockedResultEXT013.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        lockedResultEXT013.setInt("EXCHNO", ((Integer)lockedResultEXT013.get("EXCHNO") + 1))
        lockedResultEXT013.set("EXCHID", program.getUser())
        lockedResultEXT013.update()
      }
      queryEXT013.readLock(containerEXT013, updateEXT013)
    } else {
      containerEXT013.set("EXCONO", currentCompany)
      containerEXT013.set("EXORNO", orno)
      containerEXT013.set("EXPONR", ponr)
      containerEXT013.set("EXPOSX", posx)
      containerEXT013.set("EXLINO", lino)
      containerEXT013.set("EXREMK", remk)
      containerEXT013.set("EXMSCD", mscd)
      containerEXT013.set("EXFITN", fitn)
      containerEXT013.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT013.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT013.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT013.set("EXCHNO", 1)
      containerEXT013.set("EXCHID", program.getUser())
      queryEXT013.insert(containerEXT013)
    }
  }
}
