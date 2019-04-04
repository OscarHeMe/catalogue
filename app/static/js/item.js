// Doc Ready
document.addEventListener("DOMContentLoaded", function(){
    if(current_module != 'item')
        return false;

    const inputs = document.querySelectorAll('.product-input');

    for (let i=0; i < inputs.length; i++){
        console.log(inputs[i]);
        inputs[i].addEventListener('keyup',function(){
            console.log("Changed!!");
            console.log(this);
            //document.querySelector('#UserName').value = this.value;
            let payload = {
                "item_uuid" : this.getAttribute("item_uuid"),
                "source" : this.getAttribute("source"),
                "product_id" : this.value,
                "auth" : document.getElementById("auth-code").value
            };
            console.log(payload)
            fetch(url_change_id, {
                method : 'POST',
                cache: 'no-cache',
                headers : {
                    'Content-Type' : 'application/json'
                },
                body : JSON.stringify(payload)
            }).then(function(response){
                return response.json(); 
            }).then(function(myJson){
                console.log(myJson);
            })
        });
    }


});