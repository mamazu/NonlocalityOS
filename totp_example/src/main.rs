use qrcode::{render::unicode::Dense1x2, QrCode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use totp_rs::{Algorithm, Secret, TOTP};
use tracing::{error, info};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    let totp = TOTP::new(
        Algorithm::SHA1,
        6,
        1,
        30,
        Secret::Raw("TestSecretSuperSecret".as_bytes().to_vec())
            .to_bytes()
            .unwrap(),
        Some("NonlocalityOS".to_string()),
        "totp_example".to_string(),
    )
    .unwrap();

    {
        let url = totp.get_url();
        info!("URL: {}", url);

        {
            let code = QrCode::new(url.as_bytes()).unwrap();
            let string = code.render::<Dense1x2>().build();
            info!("{}", &string);
        }
    }

    let mut previous_password: Option<String> = None;
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let current_password = totp.generate(now);

        if !totp.check(&current_password, now) {
            error!("The current password is considered invalid. Something wrong with the clock?");
        }

        if previous_password.as_deref() != Some(current_password.as_str()) {
            info!("Current password: {}", &current_password);

            if let Some(previous_password_exists) = previous_password.as_deref() {
                if totp.check(previous_password_exists, now) {
                    info!("Previous password is also still valid as expected.");
                } else {
                    error!("Previous password is already invalid. Something wrong with the clock?");
                }
            }

            previous_password = Some(current_password)
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
