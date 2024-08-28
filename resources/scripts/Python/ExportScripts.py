for sc in Document.ScriptManager.GetScripts():
    scriptType = sc.Language.Language
    if scriptType == "IronPython":
        scriptExt = ".py"
    elif scriptType == "JavaScript":
        scriptExt = ".js"
    f = open("C:\\temp\\scripts\\" + sc.Name + scriptExt, 'wb')
    f.write(sc.ScriptCode)
    f.close