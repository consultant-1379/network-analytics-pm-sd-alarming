


$("#deleteCollectionsBtn").click(function() {
	canDelete = $("#deleteCollectionsError").first().text().trim();
    
	if(canDelete=="False"){
		$("#deleteCollectionsInput input").val('').blur();
		createDeleteDialog()
	}else{
		$("#deleteCollectionsInput input").val('Other').blur();
		$("#deleteCollectionsInput input").val('').blur();
	}
	
});
  
  
  
  
function createDeleteDialog(){
   $( "#dialog-deleteCollection" ).dialog({
      resizable: false,
      height: "auto",
      width: 400,
      modal: true,
	  draggable: false,
      buttons: {
        "OK": function() {
          $( this ).dialog( "close" );
		  $("#deleteCollectionsInput input").val('Other').blur();
        }
      }
    });
}

