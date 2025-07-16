/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.DelConstraint
 * Description : Delete records from the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class DelConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  private int currentCompany

  public DelConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    int zcid = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction ext030Query = database.table("EXT030").index("00").build()
    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXZCID", zcid)
    if(!ext030Query.readLock(ext030Request, ext030Updater)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> ext030Updater = { LockedResult ext030LockedResult ->
    ext030LockedResult.delete()
  }
}
