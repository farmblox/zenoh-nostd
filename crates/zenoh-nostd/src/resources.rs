use crate::{config::ZSessionConfig, io::transport::TransportLink, platform::ZLinkManager};

pub struct Resources<'res, Config>
where
    Config: ZSessionConfig + 'res,
{
    transport: Option<TransportLink<Link<'res, Config>, Config::Buff>>,
}

impl<'res, Config> Default for Resources<'res, Config>
where
    Config: ZSessionConfig + 'res,
{
    fn default() -> Self {
        Self { transport: None }
    }
}

type Link<'res, Config> = <<Config as ZSessionConfig>::LinkManager as ZLinkManager>::Link<'res>;

impl<'res, Config> Resources<'res, Config>
where
    Config: ZSessionConfig + 'res,
{
    pub fn init(
        &mut self,
        transport: TransportLink<Link<'res, Config>, Config::Buff>,
    ) -> &mut TransportLink<Link<'res, Config>, Config::Buff> {
        self.transport.insert(transport)
    }
}
