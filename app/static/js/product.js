// Doc Ready
document.addEventListener("DOMContentLoaded", function(){
    if(current_module != 'product')
        return false;

    // On change search
    document.getElementById("search").addEventListener('change',function(){
        new_url = url_change_id_active+"&q="+this.value;
        window.location.href=new_url;
    });

    const inputs = document.querySelectorAll('.product-input');
    // Add onclick event to the input
    for (let i=0; i < inputs.length; i++){
        console.log(inputs[i]);
        inputs[i].addEventListener('keyup',function(){
            console.log("Changed!!");
            console.log(this);
            // Item_uuid or product_id
            key = this.getAttribute("value_type");
            let payload = {};
            payload['product_uuid'] = this.getAttribute("product_uuid");
            payload['auth'] = document.getElementById("auth-code").value;
            payload['key'] = key;
            payload[key] = this.value;
            console.log(payload)
            fetch(url_change_id, {
                method : 'POST',
                cache: 'no-cache',
                headers : {
                    'Content-Type' : 'application/json'
                },
                body : JSON.stringify(payload)
            }).then(function(response){
                if(response.status != 200){
                    console.log(response)
                    feedback(response.statusText,response.status,"error");
                    return false;
                }
                return response.json(); 
            }).then(function(myJson){
                console.log(myJson);
            }).catch(function(err){
                console.log(err);
            })
        });
    }





});