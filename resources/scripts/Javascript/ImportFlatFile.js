MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
//function when value changes
var changeUI = function(){
	enableAll()
	var dropdownVal = $("#Dynamiccollection").first().text().trim()
	if (dropdownVal == "ON")
	{		
		$( "#addFile :first-child").css({
				"border-radius":"4px",
				"border":"1px solid #AAA",
                "background": "#fff",
                "letter-spacing": "0px",
                "cursor": "pointer",
                "letter-spacing": "0px",
				"color":"#AAA",
				"pointer-events": "none"
			});
	}
	if (dropdownVal == "OFF")
	{
		$( "#addFile :first-child").css({
				"color":"black",
				"pointer-events": ""
			});
	}
	var sysArea = $("#systemArea").text()
	if (sysArea == "(None)" || sysArea == "(Empty)" || sysArea == " (None)" || sysArea == " (Empty)" || sysArea == "") {
		disableAll()
		$("#systemAreaRequired").show();
		} else{
			$("#systemAreaRequired").hide();
			}
	var nodeType = $("#nodeType").text()
	if (nodeType == "---" || nodeType == "") {
		disableAll()
		$("#nodeTypeRequired").show();
		} else{
			$("#nodeTypeRequired").hide();
			}
	var eniqdb = $("#eniqdb").text()
	if (eniqdb == "---" || eniqdb == "") {
		disableAll()
		$("#eniqdbRequired").show();
		} else{
			$("#eniqdbRequired").hide();
			}
			
	
}

var systemArea = document.getElementById("systemArea")//callback is the function to trigger when target changes
var callback2 = function(mutations) {
changeUI()
}

var observer2 = new MutationObserver(callback2);
var opts2 = {
childList: true,
attributes: true,
characterData: true,
subtree: true
}
observer2.observe(systemArea,opts2);
changeUI()

//To check on page load
setTimeout(changeUI, 1000);

var nodeType = document.getElementById("nodeType")//callback is the function to trigger when target changes
var callback1 = function(mutations) {
changeUI()
}

var observer1 = new MutationObserver(callback1);
var opts1 = {
childList: true,
attributes: true,
characterData: true,
subtree: true
}
observer1.observe(nodeType,opts1);

var eniqdb = document.getElementById("eniqdb")//callback is the function to trigger when target changes
var callback3 = function(mutations) {
changeUI()
}

var observer3 = new MutationObserver(callback3);
var opts3 = {
childList: true,
attributes: true,
characterData: true,
subtree: true
}
observer3.observe(eniqdb,opts3);
//this is for dinamic collectin
var target = document.getElementById("Dynamiccollection")//callback is the function to trigger when target changes
var callback = function(mutations) {
changeUI()
}
var observer = new MutationObserver(callback);
var opts = {
childList: true,
attributes: true,
characterData: true,
subtree: true
}
observer.observe(target,opts);
changeUI()
//To check on page load
//setTimeout(changeUI, 1000);
function disableAll(){
		$( "#addFile :first-child").css({
				"border-radius":"4px",
				"border":"1px solid #AAA",
                "background": "#fff",
                "letter-spacing": "0px",
                "cursor": "pointer",
                "letter-spacing": "0px",
				"color":"#AAA",
				"pointer-events": "none"
			});
		$( "#fetch :first-child").css({
                "background": "#fff",
                "letter-spacing": "0px",
                "cursor": "pointer",
                "letter-spacing": "0px",
				"color":"#AAA",
				"pointer-events": "none"
			});
			$( "#dincol :first-child").css({
                "letter-spacing": "0px",
                "cursor": "pointer",
				"color":"#AAA",
                "letter-spacing": "0px",
				"pointer-events": "none"
			});
}
function enableAll(){
		$( "#addFile :first-child").css({
				"color":"black",
				"pointer-events": ""
			});
			$( "#fetch :first-child").css({
				"color":"black",
				"pointer-events": ""
			});
			$( "#dincol :first-child").css({
				"cursor": "pointer",
				"pointer-events": "",
				"color":"",
			});
}



