MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
isMarked = "MarkedEnm"


var changeUI = function(oldVal,newVal){
	var inputVal = $("#MarkedEnm").text().trim()
    if(inputVal == "True"){
		$("#deleteENM").css("display","")
		$("#saveENM").css("display","none")
	}else{
		$("#deleteENM").css("display","none")
		$("#saveENM").css("display","block")
}		
}

var target = document.getElementById(isMarked)
var oldVal = target.innerText.trim()


var callback = function(mutations) {
	newVal = $("#MarkedEnm").text().trim()
	if(oldVal!=newVal) changeUI(oldVal,newVal)
	oldVal = newVal;
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
