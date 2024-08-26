@0xf6806bf9e75be522;

struct BufferedRequest {
    body @0 :Data;
    headers @1 :List(Header);

    struct Header {
        name @0 :Text;
        value @1 :Data;
    }
}
