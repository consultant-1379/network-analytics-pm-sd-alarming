MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
//function when value changes
var changeUI = function(){
	var dropdownVal = $("#Dynamiccollection").first().text().trim()
	//alert(dropdownVal)
	if (dropdownVal == "OFF")
	{
		$("#WildcardExpression").hide();
		$("#WildcardExpression2").hide();
		$("#ExpLable2").hide();
	}
	if (dropdownVal == "ON")
	{
		$("#WildcardExpression").show();
		$("#WildcardExpression2").show();
		$("#ExpLable2").show();
	}
}
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

